/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2019, Joyent, Inc.
 */

use super::evacuate::EvacuateObjectStatus;
use crate::error::Error;
use crate::pg_db;

use std::collections::HashMap;
use std::str::FromStr;
use std::string::ToString;

use diesel::prelude::*;
use inflector::cases::titlecase::to_title_case;
use strum::IntoEnumIterator;
use uuid::Uuid;

#[derive(Debug, EnumString)]
pub enum StatusError {
    DBExists,
    LookupError,
}

pub fn get_status(uuid: Uuid) -> Result<HashMap<String, usize>, StatusError> {
    use super::evacuate::evacuateobjects::dsl::*;
    let db_name = uuid.to_string();
    let mut total_count = 0;
    let mut ret = HashMap::new();

    let conn = match pg_db::connect_db(&db_name) {
        Ok(c) => c,
        Err(_) => {
            return Err(StatusError::DBExists);
        }
    };

    let status_vec: Vec<EvacuateObjectStatus> =
        match evacuateobjects.select(status).get_results(&conn) {
            Ok(res) => res,
            Err(_) => {
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

pub fn list_jobs() -> Result<Vec<String>, Error> {
    let db_list = pg_db::list_databases()?;
    let mut ret = vec![];

    for db in db_list {
        if let Ok(job_id) = Uuid::from_str(&db) {
            ret.push(job_id.to_string());
        }
    }

    Ok(ret)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::evacuate::{self, EvacuateObject};
    use crate::pg_db;
    use crate::util;
    use quickcheck::{Arbitrary, StdThreadGen};

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
