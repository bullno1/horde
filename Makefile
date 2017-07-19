PROJECT = horde
PROJECT_DESCRIPTION = A DHT based on EpiChord
PROJECT_VERSION = 0.1.0

BUILD_DEPS = elvis_mk
DEP_PLUGINS = elvis_mk
LOCAL_DEPS = crypto

dep_elvis_mk = git https://github.com/inaka/elvis.mk.git 1.0.0

ELVIS_VERSION ?= 0.2.12
ELVIS_CONFIG_URL ?= https://github.com/inaka/elvis/releases/download/0.2.12/elvis.config

include erlang.mk

all:: rebar.config
