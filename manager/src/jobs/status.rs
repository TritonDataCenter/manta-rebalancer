/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

use super::evacuate::EvacuateObjectStatus;

use crate::jobs::evacuate::EvacuateJobDbConfig;
use crate::jobs::{JobActionDbEntry, JobDbEntry, JobState, REBALANCER_DB};
use crate::pg_db;
use rebalancer::error::Error;

use std::collections::HashMap;
use std::string::ToString;

use diesel::prelude::*;
use diesel::result::ConnectionError;
use diesel::sql_query;
use diesel::sql_types::{BigInt, Text};
use inflector::cases::titlecase::to_title_case;
use libmanta::moray::MantaObjectShark;
use serde::{Deserialize, Serialize};
use strum::IntoEnumIterator;
use uuid::Uuid;

static STATUS_COUNT_QUERY: &str = "SELECT status, count(status) \
                                   FROM  evacuateobjects  GROUP BY status";

#[derive(Debug, EnumString)]
pub enum StatusError {
    DBExists,
    LookupError,
    Unknown,
}

#[derive(QueryableByName, Debug)]
struct StatusCount {
    #[sql_type = "Text"]
    status: String,
    #[sql_type = "BigInt"]
    count: i64,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "action")]
pub enum JobStatusConfig {
    Evacuate(JobConfigEvacuate),
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum JobStatusResults {
    Evacuate(JobStatusResultsEvacuate),
}

#[derive(Debug, Deserialize, Serialize)]
pub struct JobStatus {
    pub config: JobStatusConfig,
    pub results: JobStatusResults,
    pub state: JobState,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct JobConfigEvacuate {
    from_shark: MantaObjectShark,
    // XXX: TODO Add source,
}

type JobStatusResultsEvacuate = HashMap<String, i64>;

fn get_rebalancer_db_conn() -> Result<PgConnection, StatusError> {
    pg_db::connect_or_create_db(REBALANCER_DB).map_err(|e| {
        error!("Error connecting to rebalancer DB: {}", e);
        StatusError::Unknown
    })
}

fn get_job_db_entry(uuid: &Uuid) -> Result<JobDbEntry, StatusError> {
    use crate::jobs::jobs::dsl::{id as job_id, jobs as jobs_db};

    let job_uuid = uuid.to_string();
    let conn = get_rebalancer_db_conn()?;

    jobs_db
        .filter(job_id.eq(&job_uuid))
        .first(&conn)
        .map_err(|e| {
            error!("Could not find job ({}): {}", job_uuid, e);
            StatusError::LookupError
        })
}

fn get_job_db_conn_common(uuid: &Uuid) -> Result<PgConnection, StatusError> {
    let db_name = uuid.to_string();
    pg_db::connect_db(&db_name).map_err(|e| {
        if let Error::DieselConnection(conn_err) = &e {
            if let ConnectionError::BadConnection(err) = conn_err {
                error!("Status DB connection: {}", err);
                return StatusError::DBExists;
            }
        }
        error!("Unknown status DB connection error: {}", e);
        StatusError::Unknown
    })
}

fn get_evacaute_job_status(
    uuid: &Uuid,
) -> Result<JobStatusResultsEvacuate, StatusError> {
    let mut ret = HashMap::new();
    let mut total_count: i64 = 0;
    let conn = get_job_db_conn_common(&uuid)?;

    // Unfortunately diesel doesn't have GROUP BY support yet, so we do a raw
    // query here.
    // See https://github.com/diesel-rs/diesel/issues/210
    let status_counts: Vec<StatusCount> =
        match sql_query(STATUS_COUNT_QUERY).load::<StatusCount>(&conn) {
            Ok(res) => res,
            Err(e) => {
                error!("Status DB query: {}", e);
                return Err(StatusError::LookupError);
            }
        };

    for status_count in status_counts.iter() {
        total_count += status_count.count;
        ret.insert(to_title_case(&status_count.status), status_count.count);
    }

    // The query won't return statuses with 0 counts, so add them here.
    for status_value in EvacuateObjectStatus::iter() {
        ret.entry(to_title_case(&status_value.to_string()))
            .or_insert(0);
    }

    ret.insert("Total".into(), total_count);

    Ok(ret)
}

fn get_evacuate_job_config(
    uuid: &Uuid,
) -> Result<JobConfigEvacuate, StatusError> {
    use crate::jobs::evacuate::config::dsl::config as config_table;
    let conn = get_job_db_conn_common(&uuid)?;

    let config: EvacuateJobDbConfig =
        config_table.first(&conn).map_err(|e| {
            error!("Could not find job config ({}): {}", uuid.to_string(), e);
            StatusError::LookupError
        })?;

    let from_shark: MantaObjectShark =
        serde_json::from_value(config.from_shark).map_err(|e| {
            error!(
                "Could not deserialize job config ({}): {}",
                uuid.to_string(),
                e
            );
            StatusError::Unknown
        })?;

    Ok(JobConfigEvacuate { from_shark })
}

fn get_job_status(
    uuid: &Uuid,
    action: &JobActionDbEntry,
) -> Result<JobStatusResults, StatusError> {
    match action {
        JobActionDbEntry::Evacuate => {
            Ok(JobStatusResults::Evacuate(get_evacaute_job_status(uuid)?))
        }
        _ => unreachable!(),
    }
}

fn get_job_config(
    uuid: &Uuid,
    action: &JobActionDbEntry,
) -> Result<JobStatusConfig, StatusError> {
    match action {
        JobActionDbEntry::Evacuate => {
            Ok(JobStatusConfig::Evacuate(get_evacuate_job_config(&uuid)?))
        }
        _ => unreachable!(),
    }
}

pub fn get_job(uuid: Uuid) -> Result<JobStatus, StatusError> {
    let job_entry = get_job_db_entry(&uuid)?;
    let results = get_job_status(&uuid, &job_entry.action)?;
    let config = get_job_config(&uuid, &job_entry.action)?;

    // get job config
    Ok(JobStatus {
        results,
        config,
        state: job_entry.state,
    })
}

pub fn list_jobs() -> Result<Vec<JobDbEntry>, StatusError> {
    use crate::jobs::jobs::dsl::jobs as jobs_db;

    let conn = get_rebalancer_db_conn()?;
    let job_list = match jobs_db.load::<JobDbEntry>(&conn) {
        Ok(list) => list,
        Err(e) => {
            error!("Error listing jobs: {}", e);
            return Err(StatusError::Unknown);
        }
    };

    Ok(job_list)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::jobs::evacuate::{EvacuateObject, ObjectSource};
    use crate::jobs::JobBuilder;
    use crate::pg_db;
    use quickcheck::{Arbitrary, StdThreadGen};
    use rebalancer::util;

    static NUM_OBJS: i64 = 200;

    #[test]
    fn list_job_test() {
        assert!(list_jobs().is_ok());
    }

    #[test]
    fn bad_job_id() {
        let _guard = util::init_global_logger(None);
        let uuid = Uuid::new_v4();
        assert!(get_job(uuid).is_err());
    }

    #[test]
    fn get_status_test() {
        use crate::jobs::evacuate::evacuateobjects::dsl::*;

        let _guard = util::init_global_logger(None);
        let mut g = StdThreadGen::new(10);
        let mut obj_vec = vec![];
        let config = Config::default();
        let job_builder = JobBuilder::new(config);

        let job = job_builder
            .evacuate(
                "fake_shark".to_string(),
                ObjectSource::default(),
                Some(NUM_OBJS as u64),
            )
            .commit()
            .expect("job builder");

        let job_id = job.get_id();

        let conn = pg_db::connect_or_create_db(&job_id.to_string())
            .expect("db connect");

        for _ in 0..NUM_OBJS {
            obj_vec.push(EvacuateObject::arbitrary(&mut g));
        }

        diesel::insert_into(evacuateobjects)
            .values(obj_vec.clone())
            .execute(&conn)
            .expect("diesel insert");

        let job_status = get_job(job_id).expect("get job status");
        let JobStatusResults::Evacuate(evac_job_results) = job_status.results;
        let count = *evac_job_results.get("Total").expect("Total count");

        assert_eq!(count, NUM_OBJS);
        println!("Get Status Test: {:#?}", count);
    }

    #[test]
    fn get_status_zero_value_test() {
        use crate::jobs::evacuate::evacuateobjects::dsl::*;

        let _guard = util::init_global_logger(None);
        let mut g = StdThreadGen::new(10);
        let mut obj_vec = vec![];

        let config = Config::default();
        let job_builder = JobBuilder::new(config);
        let job = job_builder
            .evacuate(
                "fake_shark".to_string(),
                ObjectSource::default(),
                Some(NUM_OBJS as u64),
            )
            .commit()
            .expect("job builder");

        let job_id = job.get_id();
        let conn = pg_db::connect_db(&job_id.to_string()).expect("db connect");

        for _ in 0..NUM_OBJS {
            let mut obj = EvacuateObject::arbitrary(&mut g);
            if obj.status == EvacuateObjectStatus::PostProcessing {
                obj.status = EvacuateObjectStatus::Assigned;
            }
            obj_vec.push(obj);
        }

        diesel::insert_into(evacuateobjects)
            .values(obj_vec.clone())
            .execute(&conn)
            .expect("diesel insert");

        let job_status = get_job(job_id).expect("get job status");
        let JobStatusResults::Evacuate(evac_job_results) = job_status.results;
        let total_count = *evac_job_results.get("Total").expect("Total count");
        let post_processing_count = *evac_job_results
            .get("Post Processing")
            .expect("Post Processing count");

        assert_eq!(total_count, NUM_OBJS);
        assert_eq!(post_processing_count, 0);
    }
}
