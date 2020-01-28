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
use inflector::cases::titlecase::to_title_case;
use strum::IntoEnumIterator;
use uuid::Uuid;

#[derive(Debug, EnumString)]
pub enum StatusError {
    DBExists,
    LookupError,
    Unknown,
}

pub fn get_status(uuid: Uuid) -> Result<HashMap<String, usize>, StatusError> {
    use super::evacuate::evacuateobjects::dsl::*;
    let db_name = uuid.to_string();
    let mut total_count = 0;
    let mut ret = HashMap::new();

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

    let status_vec: Vec<EvacuateObjectStatus> =
        match evacuateobjects.select(status).get_results(&conn) {
            Ok(res) => res,
            Err(e) => {
                error!("Status DB query: {}", e);
                return Err(StatusError::LookupError);
            }
        };

    for status_value in EvacuateObjectStatus::iter() {
        let count = status_vec.iter().filter(|s| *s == &status_value).count();
        let status_str = to_title_case(&status_value.to_string());

        ret.insert(status_str, count);
        total_count += count;
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
            return Err(StatusError::DBExists);
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

    static NUM_OBJS: u32 = 200;

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

        get_status(uuid).unwrap();
    }
}
