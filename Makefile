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
                    smf/manifests/rebalancer-agent.xml \
					smf/manifests/postgresql.xml \

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

PGVER=12.4
DEF_RPATH=/opt/local/lib:/opt/local/gcc7/x86_64-sun-solaris2.11/lib/amd64:/opt/local/gcc7/lib/amd64
REBALANCER_RPATH=/opt/postgresql/$(PGVER)/lib:$(DEF_RPATH)

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
	LD_LIBRARY_PATH=$(RELSTAGE_DIR)/root/opt/postgresql/$(PGVER)/lib:$(DEF_RPATH) \
	    $(CARGO) build --manifest-path=manager/Cargo.toml
	/usr/bin/elfedit -e \
	    "dyn:runpath $(REBALANCER_RPATH)" \
	    target/debug/rebalancer-manager
	/usr/bin/elfedit -e \
	    "dyn:runpath $(REBALANCER_RPATH)" \
	    target/debug/rebalancer-adm
	cp manager/src/config.json target/debug/

.PHONY: release
release: all pg deps/manta-scripts/.git $(SMF_MANIFESTS)
	@echo "Building $(RELEASE_TARBALL)"
	# application dir
	@mkdir -p $(RELSTAGEDIR)/root/opt/smartdc/$(NAME)/bin
	@mkdir -p $(RELSTAGEDIR)/root/opt/smartdc/$(NAME)/etc
	@mkdir -p $(RELSTAGEDIR)/root/opt/smartdc/$(NAME)/smf/manifests
	@mkdir -p $(RELSTAGEDIR)/root/opt/smartdc/$(NAME)/smf/methods
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
	cp -R \
	    $(TOP)/smf/manifests/postgresql.xml \
	    $(RELSTAGEDIR)/root/opt/smartdc/$(NAME)/smf/manifests/
	cp -R \
	    $(TOP)/smf/methods/postgresql \
	    $(RELSTAGEDIR)/root/opt/smartdc/$(NAME)/smf/methods/
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

.PHONY: pg
pg: deps/postgresql12/.git
	PATH=/usr/bin:$$PATH $(MAKE) -f Makefile.postgres \
	RELSTAGEDIR="$(RELSTAGEDIR)" \
	DEPSDIR="$(TOP)/deps" pg12

doc:
	$(CARGO) doc

clean::
	$(CARGO) clean

.PHONY: agent
agent:
	STAMP=$(STAMP) $(CARGO) build --manifest-path=agent/Cargo.toml --release

.PHONY: manager
manager:
	LD_LIBRARY_PATH=$(RELSTAGE_DIR)/root/opt/postgresql/$(PGVER)/lib:$(DEF_RPATH) \
	    $(CARGO) build --manifest-path=manager/Cargo.toml --release
	/usr/bin/elfedit -e \
	    "dyn:runpath $(REBALANCER_RPATH)" \
	    target/release/rebalancer-manager
	/usr/bin/elfedit -e \
	    "dyn:runpath $(REBALANCER_RPATH)" \
	    target/release/rebalancer-adm

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

rebalancer_adm_tests:
	RUST_LOG=remora=trace $(CARGO) test rebalancer_adm_tests

doc_tests:
	RUST_LOG=remora=trace $(CARGO) test --doc

test: agenttests jobtests managertests rebalancer_adm_tests doc_tests

#
# Included target definitions.
#
include ./deps/eng/tools/mk/Makefile.deps
ifeq ($(shell uname -s),SunOS)
    include ./deps/eng/tools/mk/Makefile.agent_prebuilt.targ
endif
include ./deps/eng/tools/mk/Makefile.smf.targ
include ./deps/eng/tools/mk/Makefile.targ
