use crate::error::Error;
use libmanta::moray::MantaObject;
use moray::{
    client::MorayClient,
    objects::{Etag, MethodOptions as ObjectMethodOptions},
};
use slog_scope;
use std::net::IpAddr;
use trust_dns_resolver::Resolver;

static MANTA_BUCKET: &str = "manta";

fn lookup_ip(host: &str) -> Result<IpAddr, Error> {
    let resolver = Resolver::from_system_conf()?;
    let response = resolver.lookup_ip(host)?;
    let ip: Vec<IpAddr> = response.iter().collect();

    Ok(ip[0])
}

pub fn create_client(
    shard: u32,
    domain: &str,
) -> Result<MorayClient, Error> {
    let domain_name = format!("{}.moray.{}", shard, domain);

    info!("Creating moray client for: {}", domain_name);

    // TODO: Lookup SRV record for port number
    // Waiting on trust-dns-resolver issue:
    // https://github.com/bluejekyll/trust-dns/issues/872
    let ip = lookup_ip(&domain_name)?;
    MorayClient::from_parts(ip, 2021, slog_scope::logger(), None)
        .map_err(Error::from)
}

pub fn put_object(
    mclient: &mut MorayClient,
    object: &MantaObject,
    etag: &str,
) -> Result<(), Error> {
    let mut opts = ObjectMethodOptions::default();
    let key = object.key.as_str();
    let value = serde_json::to_value(object)?;

    opts.etag = Etag::Specified(etag.to_string());

    trace!(
        "Updating metadata. Key: {}\nValue: {:#?}\nopts: {:?}",
        key,
        value,
        opts
    );

    mclient
        .put_object(MANTA_BUCKET, key, value, &opts, |o| {
            trace!("Object Updated: {}", o);
            Ok(())
        })
        .map_err(Error::from)
}
