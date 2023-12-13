from Compiler import Compiler
import sys

compiler = Compiler(tsv_infile="route.tsv", tsv_outfile="route_compiled.tsv", json_outfile="route.json",
                    configfile="config.ini")
compiler.validator()
print(f"{compiler.warnings} warnings and {compiler.errors} errors were detected during validation checks.")
if compiler.errors == 0:
    print("If you are not aware of any warnings listed, please stop compilation and fix them.")
    print("Otherwise, compilation will continue.")
    compiler.compiler()
else:
    print("Due to errors found in the data, compilation cannot proceed. Please fix the errors and retry compilation.")
    sys.exit(1)