#!/bin/bash
PATH=$(pwd):${PATH}
echo This script will ask for some personal movie ratings, then run ALS and make some recommendations...
rateMovies
spark-submit --driver-memory 2g MovieLensALS.py ../../resources/movies/ ./personalRatings.txt
