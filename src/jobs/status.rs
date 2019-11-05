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

use uuid::Uuid;
use diesel::SqliteConnection;
use diesel::prelude::*;

use diesel::dsl::count;


// TODO: consider doing a single query to get all objects hold them in memory
// and do the counts in rust code.
pub fn get_status(uuid: Uuid) -> Result<(), Error>{
    use super::evacuate::evacuateobjects::dsl::*;

    let conn = SqliteConnection::establish(&uuid.to_string())
        .unwrap_or_else(|_| panic!("Error connecting to {}", uuid));

    let skip_count = evacuateobjects
        .filter(status.eq(EvacuateObjectStatus::Skipped))
        .select(
            count(id)
        )
        .first::<i64>(&conn).unwrap();

    let error_count = evacuateobjects
        .filter(status.eq(EvacuateObjectStatus::Error))
        .select(
            count(id)
        )
        .first::<i64>(&conn).unwrap();

    let successful_count = evacuateobjects
        .filter(status.eq(EvacuateObjectStatus::Complete))
        .select(
            count(id)
        )
        .first::<i64>(&conn).unwrap();

    let total_count = evacuateobjects
        .select(
            count(id)
        )
        .first::<i64>(&conn).unwrap();

    println!(
        "Skipped Objects: {}\n\
        Error Objects: {}\n\
        Successful Objects: {}\n\
        Total Objects: {}\n",
        skip_count,
        error_count,
        successful_count,
        total_count,
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::evacuate::{self, EvacuateObject};
    use quickcheck::{Arbitrary, StdThreadGen};

    static NUM_OBJS: u32 = 1000;

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
            .values(obj_vec)
            .execute(&conn)
            .unwrap();

        get_status(uuid).unwrap();

    }
}