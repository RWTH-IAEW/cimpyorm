#!/bin/bash
set -o pipefail
pylint cimpyorm | sed -e 's/.00\/10/\%/g'

a=$?

case 1 in
  $(( ($a & 0) >0 )) ) echo "no error" ; exit $a ;;
  $(( ($a & 1) >0 )) ) echo "fatal message issued" ; exit $a ;;
  $(( ($a & 2) >0 )) ) echo "error message issued" ; exit 0 ;;
  $(( ($a & 4) >0 )) ) echo "warning message issued"; exit 0 ;;
  $(( ($a & 8) >0 )) ) echo "refactor message issued"; exit 0 ;;
  $(( ($a & 16) >0 )) ) echo "convention message issued"; exit 0 ;;
  $(( ($a & 32) >0 )) ) echo "usage error"; exit $a ;;
esac
