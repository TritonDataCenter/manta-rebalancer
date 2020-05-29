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

SMF_MANIFESTS =     smf/manifests/rebalancer.xml \
                    smf/manifests/rebalancer-agent.xml

AGENT_TARBALL       := $(NAME)-agent-$(STAMP).tar.gz
AGENT_MANIFEST      := $(NAME)-agent-$(STAMP).manifest
RELEASE_TARBALL     := $(NAME)-pkg-$(STAMP).tar.gz
RELSTAGEDIR         := /tmp/$(NAME)-$(STAMP)
RELSTAGEDIR_AGENT   := /tmp/$(NAME)-agent-$(STAMP)

# This image is triton-origin-x86_64-19.4.0
BASE_IMAGE_UUID = 59ba2e5e-976f-4e09-8aac-a4a7ef0395f5
BUILDIMAGE_NAME = mantav2-rebalancer
BUILDIMAGE_DESC = Manta Rebalancer
AGENTS          = amon config registrar
# Biasing to postgresql v11 over v10 to match the postgresql11-client that
# is installed in the jenkins-agent build zones for 19.4.0 (per buckets-mdapi's
# requirements).
BUILDIMAGE_PKGSRC = postgresql11-server-11.6 postgresql11-client-11.6

ENGBLD_USE_BUILDIMAGE   = true

CLEAN_FILES += rebalancer-agent-*.tar.gz rebalancer-agent-*.manifest

ifeq ($(shell uname -s),SunOS)
    include ./deps/eng/tools/mk/Makefile.agent_prebuilt.defs
endif
include ./deps/eng/tools/mk/Makefile.smf.defs

#
# Repo-specific targets
#

all: agent manager

debug:
	$(CARGO) build --manifest-path=agent/Cargo.toml
	$(CARGO) build --manifest-path=manager/Cargo.toml
	cp manager/src/config.json target/debug/

.PHONY: release
release: all deps/manta-scripts/.git $(SMF_MANIFESTS)
	@echo "Building $(RELEASE_TARBALL)"
	# application dir
	@mkdir -p $(RELSTAGEDIR)/root/opt/smartdc/$(NAME)/bin
	@mkdir -p $(RELSTAGEDIR)/root/opt/smartdc/$(NAME)/etc
	@mkdir -p $(RELSTAGEDIR)/root/opt/smartdc/$(NAME)/smf/manifests
	cp $(TOP)/etc/postgresql.conf $(RELSTAGEDIR)/root/opt/smartdc/$(NAME)/etc/
	cp -R \
	    $(TOP)/sapi_manifests \
	    $(RELSTAGEDIR)/root/opt/smartdc/$(NAME)/
	cp \
	    target/release/rebalancer-manager \
	    target/release/rebalancer-adm \
	    $(RELSTAGEDIR)/root/opt/smartdc/$(NAME)/bin/
	cp -R \
	    $(TOP)/smf/manifests/rebalancer.xml \
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
publish: release pkg_agent
	mkdir -p $(ENGBLD_BITS_DIR)/$(NAME)
	cp $(TOP)/$(RELEASE_TARBALL) $(ENGBLD_BITS_DIR)/$(NAME)/$(RELEASE_TARBALL)
	cp $(TOP)/$(AGENT_TARBALL) $(ENGBLD_BITS_DIR)/$(NAME)/$(AGENT_TARBALL)
	cp $(TOP)/$(AGENT_MANIFEST) $(ENGBLD_BITS_DIR)/$(NAME)/$(AGENT_MANIFEST)

doc:
	$(CARGO) doc

clean::
	$(CARGO) clean

.PHONY: agent
agent:
	$(CARGO) build --manifest-path=agent/Cargo.toml --release

.PHONY: manager
manager:
	$(CARGO) build --manifest-path=manager/Cargo.toml --release

.PHONY: pkg_agent
pkg_agent:
	@echo "Building $(AGENT_TARBALL)"
	# agent dir
	@mkdir -p $(RELSTAGEDIR_AGENT)/root/opt/smartdc/$(NAME)-agent/etc
	@mkdir -p $(RELSTAGEDIR_AGENT)/root/opt/smartdc/$(NAME)-agent/bin
	@mkdir -p $(RELSTAGEDIR_AGENT)/root/opt/smartdc/$(NAME)-agent/smf/manifests
	@mkdir -p $(RELSTAGEDIR_AGENT)/root/opt/smartdc/$(NAME)-agent/sapi_manifests
	cp -R \
	    $(TOP)/smf/manifests/rebalancer-agent.xml \
	    $(RELSTAGEDIR_AGENT)/root/opt/smartdc/$(NAME)-agent/smf/manifests/
	cp \
	    target/release/rebalancer-agent \
	    $(RELSTAGEDIR_AGENT)/root/opt/smartdc/$(NAME)-agent/bin/
	cp -R $(TOP)/sapi_manifests/agent \
	    $(RELSTAGEDIR_AGENT)/root/opt/smartdc/$(NAME)-agent/sapi_manifests/
	# package it up
	cd $(RELSTAGEDIR_AGENT) && $(TAR) -I pigz -cf $(TOP)/$(AGENT_TARBALL) root
	uuid -v4 > $(RELSTAGEDIR_AGENT)/image_uuid
	cat $(TOP)/agent-manifest.tmpl | sed \
	    -e "s/UUID/$$(cat $(RELSTAGEDIR_AGENT)/image_uuid)/" \
	    -e "s/NAME/$(BUILDIMAGE_NAME)-agent/" \
	    -e "s/VERSION/$(STAMP)/" \
	    -e "s/DESCRIPTION/$(BUILDIMAGE_DESC) Agent/" \
	    -e "s/BUILDSTAMP/$(STAMP)/" \
	    -e "s/SIZE/$$(stat --printf="%s" $(TOP)/$(AGENT_TARBALL))/" \
	    -e "s/SHA/$$(openssl sha1 $(TOP)/$(AGENT_TARBALL) \
	    | cut -d ' ' -f2)/" \
	    > $(TOP)/$(AGENT_MANIFEST)
	@rm -rf $(RELSTAGEDIR_AGENT)

clippy-deps:
	rustup component add clippy

managertests:
	RUST_LOG=remora=trace $(CARGO) test tests --bin rebalancer-manager -- --test-threads=1

jobtests:
	RUST_LOG=remora=trace $(CARGO) test job -- --test-threads=1
	RUST_LOG=remora=trace $(CARGO) test config -- --test-threads=1

agenttests:
	RUST_LOG=remora=trace $(CARGO) test agenttests

test: agenttests jobtests managertests

#
# Included target definitions.
#
include ./deps/eng/tools/mk/Makefile.deps
ifeq ($(shell uname -s),SunOS)
    include ./deps/eng/tools/mk/Makefile.agent_prebuilt.targ
endif
include ./deps/eng/tools/mk/Makefile.smf.targ
include ./deps/eng/tools/mk/Makefile.targ
