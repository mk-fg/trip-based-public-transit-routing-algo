#!/bin/bash
set -e

otp_jar=otp-1.0.0-shaded.jar
otp_src_dir=src # src/ dir in otp sources/repo, or unpacked jar dir
data=data/shizuoka
mem=4G

otp_dst=$(readlink -f "$otp_jar")
otp_src="$otp_dst".orig
[[ -e "$otp_src" ]] || cp -a "$otp_dst" "$otp_src"

pushd "$otp_src_dir" >/dev/null
files=( $(git status -uno --porcelain | awk '{print $NF}') )
rsync -t "$otp_src" "$otp_jar"
zip "$otp_jar" "${files[@]}"
rsync -t "$otp_jar" "$otp_dst"
popd >/dev/null

exec java -Xmx"$mem" -jar "$otp_jar" --build "$data" --inMemory
