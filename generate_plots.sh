progress_bar="pv -s 130M -l"

echo "data={}" > assets/data.js
gzip -dc $1/changesets.csv.gz | $progress_bar | python3 src/add_topic_general.py $1
gzip -dc $1/changesets.csv.gz | $progress_bar | python3 src/add_topic_editing_software.py $1
gzip -dc $1/changesets.csv.gz | $progress_bar | python3 src/add_topic_corporation.py $1
gzip -dc $1/changesets.csv.gz | $progress_bar | python3 src/add_topic_source.py $1
gzip -dc $1/changesets.csv.gz | $progress_bar | python3 src/add_topic_imagery_service.py $1
gzip -dc $1/changesets.csv.gz | $progress_bar | python3 src/add_topic_hashtags.py $1
gzip -dc $1/changesets.csv.gz | $progress_bar | python3 src/add_topic_streetcomplete.py $1
gzip -dc $1/changesets.csv.gz | $progress_bar | python3 src/add_topic_bot.py $1
gzip -dc $1/changesets.csv.gz | $progress_bar | python3 src/add_topic_changeset_tags.py $1
gzip -dc $1/changesets.csv.gz | $progress_bar | python3 src/add_topic_hotosm.py $1

python3 src/compress_datajs.py $1