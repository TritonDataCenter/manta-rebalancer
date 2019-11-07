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
use crate::util;

use std::str::FromStr;

use diesel::prelude::*;
use inflector::cases::titlecase::to_title_case;
use strum::IntoEnumIterator;
use uuid::Uuid;

pub fn get_status(uuid: Uuid) -> Result<(), Error> {
    use super::evacuate::evacuateobjects::dsl::*;
    let db_name = uuid.to_string();
    let mut total_count = 0;

    let conn = match util::connect_db(&db_name) {
        Ok(c) => c,
        Err(e) => {
            println!(
                "Error connecting to database ({}).  Is this a valid Job \
                 UUID: {}?",
                e, db_name
            );
            return Err(e);
        }
    };

    let status_vec: Vec<EvacuateObjectStatus> = evacuateobjects
        .select(status)
        .get_results(&conn)
        .expect("DB select error");

    for status_value in EvacuateObjectStatus::iter() {
        let count = status_vec.iter().filter(|s| *s == &status_value).count();

        println!("{}: {}", to_title_case(&status_value.to_string()), count);
        total_count += count;
    }

    println!("Total: {}", total_count);

    Ok(())
}

pub fn list_jobs() -> Result<(), Error> {
    let db_list = util::list_databases()?;

    for db in db_list {
        if let Ok(job_id) = Uuid::from_str(&db) {
            println!("{}", job_id);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::evacuate::{self, EvacuateObject};
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

        let conn = util::create_and_connect_db(&uuid.to_string()).unwrap();
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
