/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2019, Joyent, Inc.
 */

//use super::evacuate::EvacuateObject;
use super::evacuate::EvacuateObjectStatus;
use crate::error::Error;

use diesel::prelude::*;
use diesel::SqliteConnection;
use uuid::Uuid;

// TODO: consider doing a single query to get all objects hold them in memory
// and do the counts in rust code.
pub fn get_status(uuid: Uuid) -> Result<(), Error> {
    use super::evacuate::evacuateobjects::dsl::*;
    let mut status_vec: Vec<EvacuateObjectStatus> = vec![];
    let conn = SqliteConnection::establish(&uuid.to_string())
        .unwrap_or_else(|_| panic!("Error connecting to {}", uuid));

    for _ in 0..100 {
        match evacuateobjects.select(status).get_results(&conn) {
            Ok(ret) => {
                status_vec = ret;
                break;
            }
            Err(_) => {
                std::thread::sleep(std::time::Duration::from_millis(50));
                continue;
            }
        }
    }

    let skip_count = status_vec
        .iter()
        .filter(|s| *s == &EvacuateObjectStatus::Skipped)
        .count();

    let error_count = status_vec
        .iter()
        .filter(|s| *s == &EvacuateObjectStatus::Error)
        .count();

    let successful_count = status_vec
        .iter()
        .filter(|s| *s == &EvacuateObjectStatus::Complete)
        .count();

    let total_count = status_vec.len();

    println!(
        "Skipped Objects: {}\n\
         Error Objects: {}\n\
         Successful Objects: {}\n\
         Total Objects: {}\n",
        skip_count, error_count, successful_count, total_count,
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::evacuate::{self, EvacuateObject};
    use quickcheck::{Arbitrary, StdThreadGen};

    static NUM_OBJS: u32 = 200000;

    #[test]
    fn get_status_test() {
        use crate::jobs::evacuate::evacuateobjects::dsl::*;

        let uuid = Uuid::new_v4();
        let mut g = StdThreadGen::new(10);
        let mut obj_vec = vec![];
        let conn = SqliteConnection::establish(&uuid.to_string())
            .unwrap_or_else(|_| panic!("Error connecting to {}", uuid));

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
