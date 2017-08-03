PROJECT = horde
PROJECT_DESCRIPTION = A DHT based on EpiChord
PROJECT_VERSION = 0.1.0

LOCAL_DEPS = crypto
TEST_DEPS = cth_readable meck proper elvis_mk
DEP_PLUGINS = elvis_mk

dep_elvis_mk = git https://github.com/inaka/elvis.mk.git 1.0.0
dep_cth_readable = git https://github.com/ferd/cth_readable.git v1.3.0
dep_meck_commit = 0.8.7
dep_proper_commit = v1.2

ELVIS_VERSION ?= 0.2.12
ELVIS_CONFIG_URL ?= https://github.com/inaka/elvis/releases/download/0.2.12/elvis.config

CT_OPTS += -ct_hooks cth_readable_shell

include erlang.mk

all:: rebar.config
