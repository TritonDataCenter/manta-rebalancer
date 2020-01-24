/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

use std::fmt;
use trust_dns_resolver::error::ResolveErrorKind;

#[derive(Debug)]
pub enum Error {
    Internal(InternalError),
    IoError(std::io::Error),
    Hyper(hyper::Error),
    Diesel(diesel::result::Error),
    SerdeJson(serde_json::error::Error),
    Reqwest(reqwest::Error),
    Resolve(trust_dns_resolver::error::ResolveError),
    ParseInt(std::num::ParseIntError),
    DieselConnection(diesel::ConnectionError),
}

impl std::error::Error for Error {
    fn description(&self) -> &str {
        match self {
            Error::Internal(e) => e.msg.as_str(),
            Error::IoError(e) => e.description(),
            Error::Hyper(e) => e.description(),
            Error::Diesel(e) => e.description(),
            Error::SerdeJson(e) => e.description(),
            Error::Reqwest(e) => e.description(),
            Error::ParseInt(e) => e.description(),
            Error::DieselConnection(e) => e.description(),
            Error::Resolve(e) => {
                let kind = e.kind();
                match kind {
                    ResolveErrorKind::Message(m) => m,
                    ResolveErrorKind::Io => "IO Error",
                    ResolveErrorKind::Msg(m) => m.as_str(),
                    ResolveErrorKind::NoRecordsFound { .. } => {
                        "No Records Found"
                    }
                    ResolveErrorKind::Proto => "Proto Error",
                    ResolveErrorKind::Timeout => "Request Timed Out",
                }
            }
        }
    }
}

impl From<hyper::Error> for Error {
    fn from(error: hyper::Error) -> Self {
        Error::Hyper(error)
    }
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Error::IoError(error)
    }
}

impl From<InternalError> for Error {
    fn from(error: InternalError) -> Self {
        Error::Internal(error)
    }
}

impl From<diesel::result::Error> for Error {
    fn from(error: diesel::result::Error) -> Self {
        Error::Diesel(error)
    }
}

impl From<serde_json::error::Error> for Error {
    fn from(error: serde_json::error::Error) -> Self {
        Error::SerdeJson(error)
    }
}

impl From<reqwest::Error> for Error {
    fn from(error: reqwest::Error) -> Self {
        Error::Reqwest(error)
    }
}

impl From<trust_dns_resolver::error::ResolveError> for Error {
    fn from(error: trust_dns_resolver::error::ResolveError) -> Self {
        Error::Resolve(error)
    }
}

impl From<std::num::ParseIntError> for Error {
    fn from(error: std::num::ParseIntError) -> Self {
        Error::ParseInt(error)
    }
}

impl From<diesel::ConnectionError> for Error {
    fn from(error: diesel::ConnectionError) -> Self {
        Error::DieselConnection(error)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Internal(e) => write!(f, "{}", e),
            Error::IoError(e) => write!(f, "{}", e),
            Error::Hyper(e) => write!(f, "{}", e),
            Error::Diesel(e) => write!(f, "{}", e),
            Error::SerdeJson(e) => write!(f, "{}", e),
            Error::Reqwest(e) => write!(f, "{}", e),
            Error::Resolve(e) => write!(f, "{}", e),
            Error::ParseInt(e) => write!(f, "{}", e),
            Error::DieselConnection(e) => write!(f, "{}", e),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct InternalError {
    msg: String,
    pub code: InternalErrorCode,
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum InternalErrorCode {
    Other,
    InvalidJobAction,
    Crossbeam,
    PickerError,
    AssignmentLookupError,
    AssignmentGetError,
    LockError,
    HashNotFound,
    DuplicateShark,
    BadMantaObject,
}

impl fmt::Display for InternalError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let code = self.code as i32;
        write!(f, "Error {}: {}", code, self.msg)
    }
}

impl InternalError {
    pub fn new<S: Into<String>>(
        err_code: Option<InternalErrorCode>,
        message: S,
    ) -> Self {
        let mut code = InternalErrorCode::Other;
        let msg = message.into();

        if let Some(c) = err_code {
            code = c;
        }

        InternalError { msg, code }
    }
}

#[derive(Debug)]
pub enum CrossbeamError<T> {
    Send(crossbeam_channel::SendError<T>),
    Recv(crossbeam_channel::RecvError),
}

impl<T> From<crossbeam_channel::SendError<T>> for CrossbeamError<T> {
    fn from(error: crossbeam_channel::SendError<T>) -> Self {
        CrossbeamError::Send(error)
    }
}

impl<T> From<crossbeam_channel::RecvError> for CrossbeamError<T> {
    fn from(error: crossbeam_channel::RecvError) -> Self {
        CrossbeamError::Recv(error)
    }
}

impl<T> std::error::Error for CrossbeamError<T>
where
    T: std::fmt::Debug + Send,
{
    fn description(&self) -> &str {
        match self {
            CrossbeamError::Send(e) => e.description(),
            CrossbeamError::Recv(e) => e.description(),
        }
    }
}

impl<T> fmt::Display for CrossbeamError<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CrossbeamError::Send(e) => write!(f, "{}", e),
            CrossbeamError::Recv(e) => write!(f, "{}", e),
        }
    }
}
