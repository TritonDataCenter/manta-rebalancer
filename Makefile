#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright 2019 Joyent, Inc.
#

NAME=rebalancer

RUST_CODE = 1

ENGBLD_REQUIRE       := $(shell git submodule update --init deps/eng)
include ./deps/eng/tools/mk/Makefile.defs
TOP ?= $(error Unable to access eng.git submodule Makefiles.)

SMF_MANIFESTS =        smf/manifests/rebalancer-manager.xml

RELEASE_TARBALL      := $(NAME)-pkg-$(STAMP).tar.gz
RELSTAGEDIR          := /tmp/$(NAME)-$(STAMP)
    
# Needed for the amon-agent to use python2.x
# XXX timf just discovered this isn't enough, so
# we might need to hack up a tempdir that symlinks
# 'python' -> /opt/local/bin/python2 and add that
# to the top of $PATH. An engbld-common target that
# does that might be useful in the future
# XXX trentm trying without amon to start, can add it separately
#PYTHON = /opt/local/bin/python2

# This image is triton-origin-x86_64-19.2.0
BASE_IMAGE_UUID = a0d5f456-ba0f-4b13-bfdc-5e9323837ca7
BUILDIMAGE_NAME = mantav2-rebalancer
BUILDIMAGE_DESC = Manta Rebalancer
AGENTS          = config registrar
BUILDIMAGE_PKGSRC = postgresql11-client

ENGBLD_USE_BUILDIMAGE   = true

RUST_CLIPPY_ARGS ?= --features "postgres"

ifeq ($(shell uname -s),SunOS)
    include ./deps/eng/tools/mk/Makefile.agent_prebuilt.defs
endif
include ./deps/eng/tools/mk/Makefile.smf.defs

#
# Repo-specific targets
#

all: check doc
	cargo build --bin rebalancer-agent --release
	cargo build --bin rebalancer-manager --features "postgres" --release
	cargo build --bin rebalancer-adm --features "postgres" --release
	cp src/config.json target/release/

debug:
	cargo build --bin rebalancer-agent
	cargo build --bin rebalancer-manager --features "postgres"
	cargo build --bin rebalancer-adm --features "postgres"
	cp src/config.json target/debug/

.PHONY: release
release: all deps/manta-scripts/.git $(SMF_MANIFESTS)
	    @echo "Building $(RELEASE_TARBALL)"
		# application dir
	    @mkdir -p $(RELSTAGEDIR)/root/opt/smartdc/$(NAME)/bin
	    cp -R \
	        $(TOP)/smf \
	        $(RELSTAGEDIR)/root/opt/smartdc/$(NAME)/
	    cp \
			target/debug/rebalancer-manager \
			$(RELSTAGEDIR)/root/opt/smartdc/$(NAME)/bin/
		# boot
	    @mkdir -p $(RELSTAGEDIR)/root/opt/smartdc/boot/scripts
	    cp -R $(TOP)/deps/manta-scripts/*.sh \
	        $(RELSTAGEDIR)/root/opt/smartdc/boot/scripts/
	    cp -R $(TOP)/boot/* \
	        $(RELSTAGEDIR)/root/opt/smartdc/boot/
		# package it up
	    cd $(RELSTAGEDIR) && $(TAR) -I pigz -cf $(TOP)/$(RELEASE_TARBALL) root
	    @rm -rf $(RELSTAGEDIR)


.PHONY: publish
publish: release
	mkdir -p $(ENGBLD_BITS_DIR)/$(NAME)
	cp $(TOP)/$(RELEASE_TARBALL) $(ENGBLD_BITS_DIR)/$(NAME)/$(RELEASE_TARBALL)

agent:
	$(CARGO) build --bin rebalancer-agent

manager:
	$(CARGO) build --bin rebalancer-manager --features "postgres"

doc:
	$(CARGO) doc --features "postgres"

clean::
	$(CARGO) clean

# XXX timf, possibly only needed when I tried to build on my mac by mistake
# need to do a build as a fresh user just to make sure
clippy-deps:
	rustup component add clippy

jobtests:
	RUST_LOG=remora=trace $(CARGO) test job --features "postgres" -- --test-threads=1

agenttests:
	RUST_LOG=remora=trace $(CARGO) test agent --features "postgres"

test: agenttests jobtests

#
# Included target definitions.
#
include ./deps/eng/tools/mk/Makefile.deps
ifeq ($(shell uname -s),SunOS)
    include ./deps/eng/tools/mk/Makefile.agent_prebuilt.targ
endif
include ./deps/eng/tools/mk/Makefile.smf.targ
include ./deps/eng/tools/mk/Makefile.targ
