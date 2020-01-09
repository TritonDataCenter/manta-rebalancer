/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

use rebalancer::error::Error;

use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::result::ConnectionError;
use diesel::sql_query;

static DB_URL: &str = "postgres://postgres:postgres@";

pub fn connect_db(db_name: &str) -> Result<PgConnection, Error> {
    let connect_url = format!("{}/{}", DB_URL, db_name);
    PgConnection::establish(&connect_url).map_err(Error::from)
}

pub fn create_db(db_name: &str) -> Result<usize, Error> {
    let create_query = format!("CREATE DATABASE \"{}\"", db_name);
    let conn = PgConnection::establish(&DB_URL)?;

    conn.execute(&create_query).map_err(Error::from)
}

table! {
    use diesel::sql_types::Text;
    pg_database (datname) {
        datname -> Text,
    }
}

#[derive(QueryableByName, Debug)]
#[table_name = "pg_database"]
struct PgDatabase {
    datname: String,
}

pub fn list_databases() -> Result<Vec<String>, Error> {
    let list_query = "SELECT datname FROM pg_database";
    let conn = PgConnection::establish(&DB_URL)?;

    sql_query(list_query)
        .load::<PgDatabase>(&conn)
        .map(|res| res.iter().map(|r| r.datname.clone()).collect())
        .map_err(Error::from)
}

pub fn create_and_connect_db(db_name: &str) -> Result<PgConnection, Error> {
    create_db(db_name)?;
    connect_db(db_name)
}

pub fn connect_or_create_db(db_name: &str) -> Result<PgConnection, Error> {
    match connect_db(db_name) {
        Ok(conn) => Ok(conn),
        Err(err) => {
            if let Error::DieselConnection(derr) = err {
                if let ConnectionError::BadConnection(_) = derr {
                    create_and_connect_db(db_name)
                } else {
                    Err(Error::from(derr))
                }
            } else {
                Err(err)
            }
        }
    }
}
