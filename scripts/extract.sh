#!/bin/bash
function usage() {
	echo Usage:
	echo extract.sh full /home/zpfxellose/data/target_file 220 eform bond_fundamental_info modify_date
	echo extract.sh inc /home/zpfxellose/data/target_file /home/zpfxellose/data/latest_file 220 eform bond_fundamental_info modify_date
}

# Full Load
if [ "$#" -eq 6 ]; then 
	if [ $1 == "full" ]; then
		TARGET_FILE=$2
		DB=$3
		SCHEMA=$4
		TABLE=$5
		ORDERBY=$6
		MYSQL="mysql --login-path="$DB
		echo `date "+%y-%m-%d %H:%M:%S"` Doing full extracting on table $SCHEMA.$TABLE
	else
		usage
		exit 1
	fi
	mkdir -p `dirname $TARGET_FILE`
	$MYSQL --skip-column-names --batch --raw -e "select * from "$SCHEMA.$TABLE" order by $ORDERBY" > data/tmp/tmp.extract

	if [ -f "data/tmp/tmp.extract" ]; then
		TARGETLINES=`cat data/tmp/tmp.extract | wc -l`
		if [ "$TARGETLINES" -eq 0 ]; then
			rm data/tmp/tmp.extract
		else 
			mv data/tmp/tmp.extract $TARGET_FILE
			echo $SCHEMA.$TABLE: `cat $TARGET_FILE | wc -l ` New Lines
			echo $TARGET_FILE
		fi
	fi

	exit 0
fi

# Incremental Load
if [ "$#" -eq 7 ]; then
    if [ $1 == "inc" ]; then
            TARGET_FILE=$2
	LATEST_FILE=$3
            DB=$4
            SCHEMA=$5
            TABLE=$6
	ORDERBY=$7
	MYSQL="mysql --login-path="$DB
#                echo Doing incremental extracting on table $SCHEMA.$TABLE
    else
            usage
            exit 1
    fi

	STATUSLINES=`cat $LATEST_FILE | wc -l`

	if [[ $STATUSLINES > 0 ]] 
	then
		if [ $ORDERBY == "modify_date" ]; then
			LAST_STATUS_TIME=`tail -1 $LATEST_FILE |  awk -F'\t' '{print $4}'`

			#echo last status time: $LAST_STATUS_TIME

			LAST_STATUS_ID=`cat $LATEST_FILE | awk -F'\t' -v l="$LAST_STATUS_TIME" '{if($4 == l) printf "\""$1"\","}'`

			#echo last status id : $LAST_STATUS_ID
			mkdir -p `dirname $TARGET_FILE`
			$MYSQL --skip-column-names --batch --raw -e "select * from "$SCHEMA.$TABLE" where (modify_date = '$LAST_STATUS_TIME' and id not in (${LAST_STATUS_ID::-1})) or modify_date > '$LAST_STATUS_TIME' order by modify_date" > data/tmp/tmp.extract

		elif [ $ORDERBY == "id" ]; then
		        LAST_STATUS_ID=`tail -1 $LATEST_FILE | awk -F'\t' '{print $1}'`
			mkdir -p `dirname $TARGET_FILE`
			$MYSQL --skip-column-names --batch --raw -e "select * from "$SCHEMA.$TABLE" where id > '${LAST_STATUS_ID}' order by id" > data/tmp/tmp.extract
		else 
			echo unknown order by column $ORDERBY
		fi

	fi

	if [ -f "data/tmp/tmp.extract" ]; then
		TARGETLINES=`cat data/tmp/tmp.extract | wc -l`
		if [ "$TARGETLINES" -eq 0 ]; then
			rm data/tmp/tmp.extract
		else 
			mv data/tmp/tmp.extract $TARGET_FILE
			echo $SCHEMA.$TABLE: `cat $TARGET_FILE | wc -l ` New Lines
			echo $TARGET_FILE
		fi
	fi
	exit 0
fi

usage
