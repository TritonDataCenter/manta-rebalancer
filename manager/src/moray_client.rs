/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

use libmanta::moray::MantaObjectShark;
use moray::{
    client::MorayClient,
    objects::{Etag, MethodOptions as ObjectMethodOptions},
};
use rand::seq::SliceRandom;
use rebalancer::error::{Error, InternalError, InternalErrorCode};
use serde_json::Value;
use slog_scope;
use std::net::{IpAddr, SocketAddr};

static MANTA_BUCKET: &str = "manta";
static MANTA_STORAGE_BUCKET: &str = "manta_storage";
static MANTA_STORAGE_BUCKET_SHARD: u32 = 1;
static MANTA_STORAGE_ID: &str = "manta_storage_id";

// We can't use trust-dns-resolver here because it uses futures with a
// block_on, and calling a block_on from within a block_on is not allowed.
use resolve::resolve_host;
use resolve::{record::Srv, DnsConfig, DnsResolver};

// Get the SRV record which gives us the target and port of the moray service.
fn get_srv_record(svc: &str, proto: &str, host: &str) -> Result<Srv, Error> {
    let query = format!("{}.{}.{}", svc, proto, host);
    let r = DnsResolver::new(DnsConfig::load_default()?)?;
    match r
        .resolve_record::<Srv>(&query)?
        .choose(&mut rand::thread_rng())
    {
        Some(rec) => {
            dbg!(&rec);
            Ok(rec.clone())
        }
        None => Err(InternalError::new(
            Some(InternalErrorCode::IpLookupError),
            "SRV Lookup returned success with 0 IPs",
        )
        .into()),
    }
}

fn lookup_ip(host: &str) -> Result<IpAddr, Error> {
    match resolve_host(host)?.collect::<Vec<IpAddr>>().first() {
        Some(a) => Ok(*a),
        None => Err(InternalError::new(
            Some(InternalErrorCode::IpLookupError),
            "IP Lookup returned success with 0 IPs",
        )
        .into()),
    }
}

fn get_moray_srv_sockaddr(host: &str) -> Result<SocketAddr, Error> {
    let srv_record = get_srv_record("_moray", "_tcp", &host)?;
    dbg!(&srv_record);

    let ip = lookup_ip(&srv_record.target)?;

    Ok(SocketAddr::new(ip, srv_record.port))
}

// Create a moray client using the shard and the domain name only.  This will
// query binder for the SRV record for us.
pub fn create_client(shard: u32, domain: &str) -> Result<MorayClient, Error> {
    let domain_name = format!("{}.moray.{}", shard, domain);
    debug!("Creating moray client for: {}", domain_name);

    let sock_addr = get_moray_srv_sockaddr(&domain_name)?;
    trace!("Found SockAddr for moray: {:#?}", &sock_addr);

    MorayClient::new(sock_addr, slog_scope::logger(), None).map_err(Error::from)
}

// Create a moray client, and then lookup the MantaObjectShard entry from the
// storage id.
pub fn get_manta_object_shark(
    storage_id: &str,
    domain: &str,
) -> Result<MantaObjectShark, Error> {
    let mut mclient = create_client(MANTA_STORAGE_BUCKET_SHARD, domain)?;
    let mut ret: Option<MantaObjectShark> = None;
    let mut shark_id = storage_id.to_string();
    let opts = ObjectMethodOptions::default();

    if !storage_id.contains(domain) {
        shark_id = format!("{}.{}", storage_id, domain);
        warn!(
            "Domain \"{}\" not found in storage_id:\"{}\", using \"{}\"",
            domain, storage_id, shark_id
        );
    }

    let filter = format!("{}={}", MANTA_STORAGE_ID, shark_id);

    mclient.find_objects(MANTA_STORAGE_BUCKET, &filter, &opts, |o| {
        let manta_storage_id: String =
            serde_json::from_value(o.value[MANTA_STORAGE_ID].clone())?;
        let datacenter = serde_json::from_value(o.value["datacenter"].clone())?;
        ret = Some(MantaObjectShark {
            manta_storage_id,
            datacenter,
        });
        Ok(())
    })?;

    match ret {
        Some(mos) => Ok(mos),
        None => {
            let msg = format!("Error: Could not find shark {}", storage_id);
            error!("{}", &msg);
            Err(Error::from(InternalError::new(
                Some(InternalErrorCode::SharkNotFound),
                msg,
            )))
        }
    }
}

pub fn put_object(
    mclient: &mut MorayClient,
    object: &Value,
    etag: &str,
) -> Result<(), Error> {
    let mut opts = ObjectMethodOptions::default();
    let key = match object.get("key") {
        Some(k) => match serde_json::to_string(k) {
            Ok(ky) => ky.replace("\"", ""),
            Err(e) => {
                error!(
                    "Could not parse key field in object {:#?} ({})",
                    object, e
                );
                return Err(InternalError::new(
                    Some(InternalErrorCode::BadMantaObject),
                    "Could not parse Manta Object Key",
                )
                .into());
            }
        },
        None => {
            error!("Missing key field in object {:#?}", object);
            return Err(InternalError::new(
                Some(InternalErrorCode::BadMantaObject),
                "Missing Manta Object Key",
            )
            .into());
        }
    };

    opts.etag = Etag::Specified(etag.to_string());

    trace!(
        "Updating metadata. Key: {}\nValue: {:#?}\nopts: {:?}",
        key,
        object,
        opts
    );

    mclient
        .put_object(MANTA_BUCKET, &key, object.to_owned(), &opts, |o| {
            trace!("Object Updated: {}", o);
            Ok(())
        })
        .map_err(Error::from)
}
