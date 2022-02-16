VERBOSE	?= 0

VERSION	= $(shell grep VERSION lp/init.lua	\
	|awk '{print $$3}'				\
	|sed "s/[,']//g"				\
)


SPEC_NAME 	= lp-$(VERSION).rockspec

DOCKER_VERSIONS	= \
	2.8.3 		\
	2.8
DOCKER_LATEST = 2.8.3

GITVERSION	= $(shell git describe)

all:
	@echo usage: 'make test'


test:
	@echo '# Run tests for version: $(VERSION)'
	prove -r$(shell if test "$(VERBOSE)" -gt 0; then echo v; fi) t


update-spec: $(SPEC_NAME)


$(SPEC_NAME): $(lp/init.lua) lp.rockspec.in
	rm -fr lp-*.rockspec
	cp -v lp.rockspec.in $@.prepare
	sed -Ei 's/@@VERSION@@/$(VERSION)/g' $@.prepare
	mv -v $@.prepare $@
	git add $@


upload: update-spec
	rm -f lp-*.src.rock
	luarocks upload $(SPEC_NAME)	


dockers:
	@set -e; \
	cd docker; \
	for version in $(DOCKER_VERSIONS); do \
		TAGS="-t unera/tarantool-lp:$$version-$(GITVERSION)"; \
		test $$version = $(DOCKER_LATEST) && TAGS="-t unera/tarantool-lp:latest $$TAGS"; \
		echo "\\nDockers creating: $$TAGS..."; \
		sed -E "s/@@VERSION@@/$$version/g" Dockerfile.in > Dockerfile \
			| docker build . \
				$$TAGS 2>&1 |sed -u -E 's/^/\t/' \
		; \
	done

docker-upload: # dockers
	@set -e; \
	cd docker; \
	for version in $(DOCKER_VERSIONS); do \
		TAGS="unera/tarantool-lp:$$version-$(GITVERSION)"; \
		test $$version = $(DOCKER_LATEST) && TAGS="$$TAGS unera/tarantool-lp:latest"; \
		echo "\\n/ $$version / Uploading: $$TAGS..."; \
		for tag in $$TAGS; do \
			echo + docker push $$tag; \
			docker push $$tag; \
		done; \
	done


.PHONY: \
	all \
	test \
	update-spec

