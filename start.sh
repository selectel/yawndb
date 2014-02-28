#!/bin/sh
exec erl -pa ebin deps/*/ebin \
    -sname yawndb +K true +A 10 +P 300000 \
    -s yawndb \
    -kernel inet_dist_use_interface "{127,0,0,1}" \
    -yconfig ./priv/yawndb.yml
