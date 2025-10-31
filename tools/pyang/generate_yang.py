#! /usr/bin/env python3
import pathlib, os
import argparse, glob
import jinja2

def run(args):
    if not args.out_yang_dir.exists():
        args.out_yang_dir.mkdir(parents=True, exist_ok=True)
    if not args.out_cvlyang_dir.exists():
        args.out_cvlyang_dir.mkdir(parents=True, exist_ok=True)

    env = jinja2.Environment(loader=jinja2.FileSystemLoader(args.templates), trim_blocks=True)
    for fname in glob.glob(args.templates + "/*.yang.j2"):
        bfname = os.path.basename(fname)
        template = env.get_template(bfname)
        yang_model = template.render(yang_model_type="py")
        yang_path = args.out_yang_dir / bfname.strip(".j2")
        yang_path.write_text(yang_model)

        cvlyang_model = template.render(yang_model_type="cvl")
        cvlyang_path = args.out_cvlyang_dir / bfname.strip(".j2")
        cvlyang_path.write_text(cvlyang_model)

if __name__== "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--out-yang-dir', dest='out_yang_dir', help='Output directory for yang files', required=True, type=pathlib.Path)
    parser.add_argument('--out-cvlyang-dir', dest='out_cvlyang_dir', help='Output directory for CVL yang files', required=True, type=pathlib.Path)
    parser.add_argument('--templates', dest='templates', help='tar file containing yang template files', required=True, type=str)
    args = parser.parse_args()
    run(args)
