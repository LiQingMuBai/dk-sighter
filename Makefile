SHELL := /bin/sh

BIN_DIR ?= bin

CMD_DIRS := $(shell ls -1 cmd)
BUILD_TARGETS := $(addprefix build-,$(CMD_DIRS))

.PHONY: all build clean $(BUILD_TARGETS)

all: build

build:
	@mkdir -p $(BIN_DIR)
	@set -e; for d in $(CMD_DIRS); do \
		echo "build $$d"; \
		go build -o $(BIN_DIR)/$$d ./cmd/$$d; \
	done

$(BUILD_TARGETS): build-%:
	@mkdir -p $(BIN_DIR)
	@echo "build $*"
	@go build -o $(BIN_DIR)/$* ./cmd/$*

clean:
	@rm -rf $(BIN_DIR)
