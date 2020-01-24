#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright 2020 Joyent, Inc.
#

NAME=rebalancer

RUST_CODE = 1

ENGBLD_REQUIRE       := $(shell git submodule update --init deps/eng)
include ./deps/eng/tools/mk/Makefile.defs
TOP ?= $(error Unable to access eng.git submodule Makefiles.)

SMF_MANIFESTS =     smf/manifests/rebalancer-manager.xml \
                    smf/manifests/rebalancer-agent.xml

AGENT_TARBALL       := $(NAME)-agent-$(STAMP).tar.gz
RELEASE_TARBALL     := $(NAME)-pkg-$(STAMP).tar.gz
RELSTAGEDIR         := /tmp/$(NAME)-$(STAMP)

# This image is triton-origin-x86_64-19.2.0
BASE_IMAGE_UUID = a0d5f456-ba0f-4b13-bfdc-5e9323837ca7
BUILDIMAGE_NAME = mantav2-rebalancer
BUILDIMAGE_DESC = Manta Rebalancer
AGENTS          = amon config registrar
# Biasing to postgresql v11 over v10 to match the postgresql11-client that
# is installed in the jenkins-agent build zones for 19.2.0 (per buckets-mdapi's
# requirements).
BUILDIMAGE_PKGSRC = postgresql11-server-11.4 postgresql11-client-11.4

ENGBLD_USE_BUILDIMAGE   = true

RUST_CLIPPY_ARGS ?= --features "postgres"

CLEAN_FILES += rebalancer-agent-*.tar.gz

ifeq ($(shell uname -s),SunOS)
    include ./deps/eng/tools/mk/Makefile.agent_prebuilt.defs
endif
include ./deps/eng/tools/mk/Makefile.smf.defs

#
# Repo-specific targets
#

all:
	$(CARGO) build --manifest-path=agent/Cargo.toml --release
	$(CARGO) build --manifest-path=manager/Cargo.toml --release

debug:
	$(CARGO) build --manifest-path=agent/Cargo.toml
	$(CARGO) build --manifest-path=manager/Cargo.toml
	cp manager/src/config.json target/debug/

.PHONY: release
release: all deps/manta-scripts/.git $(SMF_MANIFESTS)
	@echo "Building $(RELEASE_TARBALL)"
	# application dir
	@mkdir -p $(RELSTAGEDIR)/root/opt/smartdc/$(NAME)/bin
	@mkdir -p $(RELSTAGEDIR)/root/opt/smartdc/$(NAME)/smf/manifests
	cp -R \
	    $(TOP)/sapi_manifests \
	    $(RELSTAGEDIR)/root/opt/smartdc/$(NAME)/
	cp \
	    target/release/rebalancer-manager \
	    target/release/rebalancer-adm \
	    $(RELSTAGEDIR)/root/opt/smartdc/$(NAME)/bin/
	cp -R \
	    $(TOP)/smf/manifests/rebalancer-manager.xml \
	    $(RELSTAGEDIR)/root/opt/smartdc/$(NAME)/smf/manifests/
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

doc:
	$(CARGO) doc --features "postgres"

clean::
	$(CARGO) clean

.PHONY: agent
agent:
	$(CARGO) build --bin rebalancer-agent --release

.PHONY: pkg_agent
pkg_agent:
	@echo "Building $(AGENT_TARBALL)"
	# agent dir
	@mkdir -p $(RELSTAGEDIR)/root/opt/smartdc/$(NAME)-agent/bin
	@mkdir -p $(RELSTAGEDIR)/root/opt/smartdc/$(NAME)-agent/smf/manifests
	cp -R \
	    $(TOP)/smf/manifests/rebalancer-agent.xml \
	    $(RELSTAGEDIR)/root/opt/smartdc/$(NAME)-agent/smf/manifests/
	cp \
	    target/release/rebalancer-agent \
	    $(RELSTAGEDIR)/root/opt/smartdc/$(NAME)-agent/bin/
	# package it up
	cd $(RELSTAGEDIR) && $(TAR) -I pigz -cf $(TOP)/$(AGENT_TARBALL) root
	@rm -rf $(RELSTAGEDIR)

clippy-deps:
	rustup component add clippy

jobtests:
	RUST_LOG=remora=trace $(CARGO) test job -- --test-threads=1

agenttests:
	RUST_LOG=remora=trace $(CARGO) test agenttests

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
