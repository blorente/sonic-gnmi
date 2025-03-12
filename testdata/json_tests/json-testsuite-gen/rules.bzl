def test_tar_file(name, src, visibility = None):
    """Generates JSON test files from specification.

    Args:
      name: The name of the rule.
      src: The test specification.
      visibility: Standard blaze visibility parameter, passed through to
                  subsequent rules.
    """
    out_files = [name + ".tar"]
    srcs = [src]
    cmd = ("mkdir $$$$.tmp ; " + "cp $(SRCS) $$$$.tmp/ ; " + "cd $$$$.tmp ; " +
           ("../$(location //testdata/json_tests/json-testsuite-gen:json-testsuite-gen) " +
            "-in " + (" ".join(srcs[:])) +
            " ; ") +
           "tar  --transform 's,^,./testdata/json_tests/generated/,' -cvf "+name+".tar gentest_*.json &> /dev/null;" +
           "cd .. ; " +
           "cp $$$$.tmp/"+name+".tar $(@D)/;" +
           "rm -rf $$$$.tmp")
    native.genrule(
        name = name,
        srcs = srcs,
        outs = out_files,
        cmd = cmd,
        heuristic_label_expansion = 0,
        tools = [
            "//testdata/json_tests/json-testsuite-gen:json-testsuite-gen",
        ],
        visibility = visibility,
    )