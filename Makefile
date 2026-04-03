INSTALL := /usr/bin/install

BUILD_DIR := build/bin
BAZEL_OPTS := --verbose_failures

all: init sonic-telemetry clients

sonic-telemetry: $(MAKEFILE_LIST) $(GO_DEPS)
	echo '***BEGIN TELEMETRY BUILD***'
	bazel build $(BAZEL_OPTS) telemetry:telemetry --linkopt=-lusb-1.0
	echo '***END TELEMETRY BUILD***'

clients:
	echo '***BEGIN CLIENT BUILD***'
	bazel build $(BAZEL_OPTS) dialout/dialout_client_cli:dialout_client_cli
	bazel build $(BAZEL_OPTS) gnoi_client:gnoi_client
	bazel build $(BAZEL_OPTS) third_party:gnmi_get
	bazel build $(BAZEL_OPTS) third_party:gnmi_set
	bazel build $(BAZEL_OPTS) third_party:gnmi_cli
# bazel build $(BAZEL_OPTS) third_party:grpc_cli
	bazel build $(BAZEL_OPTS) @pcre_archive//:pcre_shared
	echo '***END CLIENT BUILD***'

clean:
	echo '***BEGIN CLEANUP PHASE***'
	$(RM) -r build
	echo '***END CLEANUP PHASE***'

# TODO BL: ask if not having install targets is fine.
# TODO(bazel-ready): Create bazel run target for installation.
# install:
# 	echo '***BEGIN INSTALLATION PHASE***'
# 	install -D bazel-bin/telemetry/telemetry_/telemetry $(DESTDIR)/usr/sbin/telemetry
# 	install -D bazel-bin/dialout/dialout_client_cli/dialout_client_cli_/dialout_client_cli $(DESTDIR)/usr/sbin/dialout_client_cli
# 	install -D bazel-bin/gnoi_client/gnoi_client_/gnoi_client $(DESTDIR)/usr/sbin/gnoi_client
# 	install -D bazel-bin/external/com_github_google_gnxi/gnmi_get/gnmi_get_/gnmi_get $(DESTDIR)/usr/sbin/gnmi_get
# 	install -D bazel-bin/external/com_github_google_gnxi/gnmi_set/gnmi_set_/gnmi_set $(DESTDIR)/usr/sbin/gnmi_set
# 	install -D bazel-bin/external/com_github_openconfig_gnmi/cmd/gnmi_cli/gnmi_cli_/gnmi_cli $(DESTDIR)/usr/sbin/gnmi_cli
# #	install -D $(DESTDIR)/usr/sbin/grpc_cli
# 	install -D bazel-bin/external/pcre_archive/pcre_shared/lib/libpcre.so      $(DESTDIR)/usr/lib/x86_64/libpcre.so
# 	install -D bazel-bin/external/pcre_archive/pcre_shared/lib/libpcrecpp.so   $(DESTDIR)/usr/lib/x86_64/libpcrecpp.so
# 	install -D bazel-bin/external/pcre_archive/pcre_shared/lib/libpcreposix.so $(DESTDIR)/usr/lib/x86_64/libpcreposix.so
# 	mkdir -p $(DESTDIR)/usr/bin/
	# echo '***END INSTALLATION PHASE***'


uninstall:
	echo '***BEGIN UNINSTALLATION PHASE***'
	rm $(DESTDIR)/usr/sbin/telemetry
	rm $(DESTDIR)/usr/sbin/dialout_client_cli
	rm $(DESTDIR)/usr/sbin/gnoi_client
	rm $(DESTDIR)/usr/sbin/gnmi_get
	rm $(DESTDIR)/usr/sbin/gnmi_set
	rm $(DESTDIR)/usr/sbin/gnmi_cli
# rm $(DESTDIR)/usr/sbin/grpc_cli
	echo '***END UNINSTALLATION PHASE***'

init :
	echo '***BEGIN INIT PHASE***'
	echo '***END INIT PHASE***'
