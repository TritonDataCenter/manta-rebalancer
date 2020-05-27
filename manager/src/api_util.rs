use failure::Error;
use futures::{Future, Stream};
use gotham::handler::{HandlerError, IntoHandlerError};
use gotham::state::{FromState, State};
use hyper::{Body, StatusCode};
use serde::de::DeserializeOwned;

pub trait JsonBody {
    fn json_body<'de, T: 'de>(
        &mut self,
    ) -> Box<dyn Future<Item = T, Error = HandlerError> + 'de>
    where
        T: DeserializeOwned;
}

impl JsonBody for State {
    fn json_body<'de, T: 'de>(
        &mut self,
    ) -> Box<dyn Future<Item = T, Error = HandlerError> + 'de>
    where
        T: DeserializeOwned,
    {
        let f = Body::take_from(self)
            .concat2()
            .map_err(Error::from)
            .then(|body| match body {
                Ok(valid_body) => {
                    match serde_json::from_slice(&valid_body.into_bytes()) {
                        Ok(parsed) => Ok(parsed),
                        Err(err) => Err(Error::from(err)),
                    }
                }
                Err(err) => Err(err),
            })
            .map_err(|err| {
                HandlerError::with_status(
                    err.compat().into_handler_error(),
                    StatusCode::UNPROCESSABLE_ENTITY,
                )
            });

        Box::new(f)
    }
}
