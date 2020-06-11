/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

/*
 * MIT License
 *
 * Copyright (c) 2018 Christoph Wurst
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
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
