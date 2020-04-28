/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

use super::evacuate::EvacuateObjectStatus;

use crate::jobs::{JobDbEntry, REBALANCER_DB};
use crate::pg_db;
use rebalancer::error::Error;

use std::collections::HashMap;
use std::string::ToString;

use diesel::prelude::*;
use diesel::result::ConnectionError;
use diesel::sql_query;
use diesel::sql_types::{BigInt, Text};
use inflector::cases::titlecase::to_title_case;
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

pub fn get_status(uuid: Uuid) -> Result<HashMap<String, i64>, StatusError> {
    let db_name = uuid.to_string();
    let mut ret = HashMap::new();
    let mut total_count: i64 = 0;
    let conn = match pg_db::connect_db(&db_name) {
        Ok(c) => c,
        Err(e) => {
            if let Error::DieselConnection(conn_err) = &e {
                if let ConnectionError::BadConnection(err) = conn_err {
                    error!("Status DB connection: {}", err);
                    return Err(StatusError::DBExists);
                }
            }

            error!("Unknown status DB connection error: {}", e);
            return Err(StatusError::Unknown);
        }
    };

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

pub fn list_jobs() -> Result<Vec<JobDbEntry>, StatusError> {
    use crate::jobs::jobs::dsl::jobs as jobs_db;

    let conn = match pg_db::connect_or_create_db(REBALANCER_DB) {
        Ok(conn) => conn,
        Err(e) => {
            error!("Error connecting to rebalancer DB: {}", e);
            return Err(StatusError::Unknown);
        }
    };

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
    use crate::jobs::evacuate::{self, EvacuateObject};
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
        let _guard = util::init_global_logger();
        let uuid = Uuid::new_v4();
        assert!(get_status(uuid).is_err());
    }

    #[test]
    fn get_status_test() {
        use crate::jobs::evacuate::evacuateobjects::dsl::*;

        let _guard = util::init_global_logger();
        let uuid = Uuid::new_v4();
        let mut g = StdThreadGen::new(10);
        let mut obj_vec = vec![];
        let conn = pg_db::create_and_connect_db(&uuid.to_string()).unwrap();

        evacuate::create_evacuateobjects_table(&conn).unwrap();

        for _ in 0..NUM_OBJS {
            obj_vec.push(EvacuateObject::arbitrary(&mut g));
        }

        diesel::insert_into(evacuateobjects)
            .values(obj_vec.clone())
            .execute(&conn)
            .unwrap();

        let count = get_status(uuid.clone()).unwrap();

        assert_eq!(*count.get("Total").unwrap(), NUM_OBJS);
        println!("Get Status Test: {:#?}", count);
    }

    #[test]
    fn get_status_zero_value_test() {
        use crate::jobs::evacuate::evacuateobjects::dsl::*;

        let _guard = util::init_global_logger();
        let uuid = Uuid::new_v4();
        let mut g = StdThreadGen::new(10);
        let mut obj_vec = vec![];
        let conn = pg_db::create_and_connect_db(&uuid.to_string()).unwrap();

        evacuate::create_evacuateobjects_table(&conn).unwrap();

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
            .unwrap();

        let count = get_status(uuid.clone()).unwrap();

        assert_eq!(*count.get("Total").unwrap(), NUM_OBJS);
        assert_eq!(*count.get("Post Processing").unwrap(), 0);
        println!("Zero Value Test: {:#?}", count);
    }
}
