use crate::error::{Error, InternalError, InternalErrorCode};
use libmanta::moray::MantaObjectShark;
use moray::{
    client::MorayClient,
    objects::{Etag, MethodOptions as ObjectMethodOptions},
};
use serde_json::Value;
use slog_scope;
use std::net::IpAddr;
use trust_dns_resolver::Resolver;

static MANTA_BUCKET: &str = "manta";
static MANTA_STORAGE_BUCKET: &str = "manta_storage";
static MANTA_STORAGE_ID: &str = "manta_storage_id";

fn lookup_ip(host: &str) -> Result<IpAddr, Error> {
    let resolver = Resolver::from_system_conf()?;
    let response = resolver.lookup_ip(host)?;
    let ip: Vec<IpAddr> = response.iter().collect();

    Ok(ip[0])
}

pub fn create_client(shard: u32, domain: &str) -> Result<MorayClient, Error> {
    let domain_name = format!("{}.moray.{}", shard, domain);

    info!("Creating moray client for: {}", domain_name);

    // TODO: Lookup SRV record for port number
    // Waiting on trust-dns-resolver issue:
    // https://github.com/bluejekyll/trust-dns/issues/872
    let ip = lookup_ip(&domain_name)?;
    MorayClient::from_parts(ip, 2021, slog_scope::logger(), None)
        .map_err(Error::from)
}

pub fn get_manta_object_shark(
    storage_id: &str,
    domain: &str,
) -> Result<MantaObjectShark, Error> {
    let mut mclient = create_client(1, domain)?;
    let filter = format!("{}={}", MANTA_STORAGE_ID, storage_id);
    let opts = ObjectMethodOptions::default();
    let mut ret = MantaObjectShark::default();

    mclient.find_objects(MANTA_STORAGE_BUCKET, &filter, &opts, |o| {
        ret.manta_storage_id =
            serde_json::from_value(o.value[MANTA_STORAGE_ID].clone())?;
        ret.datacenter = serde_json::from_value(o.value["datacenter"].clone())?;
        Ok(())
    })?;

    Ok(ret)
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
