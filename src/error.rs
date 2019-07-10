// Copyright 2019 Joyent, Inc.

use std::fmt;

#[derive(Debug)]
pub enum Error {
    Internal(InternalError),
    IoError(std::io::Error),
}

impl std::error::Error for Error {
    fn description(&self) -> &str {
        match self {
            Error::Internal(e) => e.msg.as_str(),
            Error::IoError(e) => e.description(),
        }
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

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Internal(e) => write!(f, "{}", e),
            Error::IoError(e) => write!(f, "{}", e),
        }
    }
}

#[derive(Debug)]
pub struct InternalError {
    msg: String,
    code: InternalErrorCode,
}

#[derive(Debug, Copy, Clone)]
pub enum InternalErrorCode {
    Other = 0,
    InvalidJobAction = 1,
    Crossbeam = 2,
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
