import snowflake.connector
import os
from datetime import datetime

def connect_snowflake():
    conn = snowflake.connector.connect(
        user=os.environ.get('SNOWFLAKE_USER'),
        password=os.environ.get('SNOWFLAKE_PASSWORD'),
        account=os.environ.get('SNOWFLAKE_ACCOUNT'),
        warehouse=os.environ.get('SNOWFLAKE_WAREHOUSE'),
        database=os.environ.get('SNOWFLAKE_DATABASE'),
        schema=os.environ.get('SNOWFLAKE_SCHEMA')
    )
    return conn

def create_raw_data_tables():
    conn = connect_snowflake()
    cur = conn.cursor()
    try:
        cur.execute("BEGIN")

        create_sequence_commands = [
            'CREATE OR REPLACE SEQUENCE id_seq_1 START WITH 1 INCREMENT BY 1;', 
            'CREATE OR REPLACE SEQUENCE id_seq_2 START WITH 1 INCREMENT BY 1;',
            'CREATE OR REPLACE SEQUENCE id_seq_3 START WITH 1 INCREMENT BY 1;',
            'CREATE OR REPLACE SEQUENCE id_seq_4 START WITH 1 INCREMENT BY 1;'
            ]
        for cmd in create_sequence_commands:
            cur.execute(cmd)
        
        create_table_command = f"\
            CREATE OR REPLACE TABLE fc_online.raw_data.match_detail (\
            id integer default id_seq_1.nextval primary key,\
            matchId varchar(100),\
            seasonId integer,\
            matchResult varchar(32),\
            matchEndType integer,\
            systemPause integer,\
            foul integer,\
            injury integer,\
            redCards integer,\
            yellowCards integer,\
            dribble integer,\
            cornerKick integer,\
            possession integer,\
            offsideCount integer,\
            averageRating float,\
            controller varchar(64)\
            );\
            "
        cur.execute(create_table_command)

        create_table_command = f"\
            CREATE OR REPLACE TABLE fc_online.raw_data.pass (\
            id integer default id_seq_3.nextval primary key,\
            matchId varchar(100),\
            passTry integer,\
            passSuccess integer,\
            shortPassTry integer,\
            shortPassSuccess integer,\
            longPassTry integer,\
            longPassSuccess integer,\
            bouncingLobPassTry integer,\
            bouncingLobPassSuccess integer,\
            drivenGroundPassTry integer,\
            drivenGroundPassSuccess integer,\
            throughPassTry integer,\
            throughPassSuccess integer,\
            lobbedThroughPassTry integer,\
            lobbedThroughPassSuccess integer\
            );\
            "
        cur.execute(create_table_command)

        create_table_command = f"\
            CREATE OR REPLACE TABLE fc_online.raw_data.shoot (\
            id integer default id_seq_2.nextval primary key,\
            matchId varchar(100),\
            shootTotal integer,\
            effectiveShootTotal integer,\
            shootOutScore integer,\
            goalTotal integer,\
            goalTotalDisplay integer,\
            ownGoal integer,\
            shootHeading integer,\
            goalHeading integer,\
            shootFreekick integer,\
            goalFreekick integer,\
            shootInPenalty integer,\
            goalInPenalty integer,\
            shootOutPenalty integer,\
            goalOutpenalty integer,\
            shootPenaltyKick integer,\
            goalPenaltykick integer\
            );\
            "
        cur.execute(create_table_command)

        create_table_command = f"\
            CREATE OR REPLACE TABLE fc_online.raw_data.defence (\
            id integer default id_seq_4.nextval primary key,\
            matchId varchar(100),\
            blockTry integer,\
            blockSuccess integer,\
            tackleTry integer,\
            tackleSuccess integer\
            );\
            "
        cur.execute(create_table_command)

        cur.execute("COMMIT")
    except Exception as e:
        cur.execute("ROLLBACK")
        print(f"Error: {e}")
    finally:
        cur.close()
        conn.close()

def copy_from_s3():
    conn = connect_snowflake()
    cur = conn.cursor()
    try:
        cur.execute("BEGIN")

        copy_command = f"\
            COPY INTO fc_online.raw_data.match_detail (matchId, seasonId, matchResult, matchEndType, systemPause, foul, injury, redCards, yellowCards, dribble, cornerKick, possession, offsideCount, averageRating, controller)\
            FROM 's3://fc-online-match/match_detail.csv'\
            credentials=(AWS_KEY_ID='{os.environ.get('EC2_ACCESS_KEY')}' AWS_SECRET_KEY='{os.environ.get('EC2_SECRET_KEY')}')\
            FILE_FORMAT = (type='CSV' skip_header=1);\
            "  
        cur.execute(copy_command)

        copy_command = f"\
            COPY INTO fc_online.raw_data.shoot (matchId, shootTotal, effectiveShootTotal, shootOutScore, goalTotal, goalTotalDisplay, ownGoal, shootHeading, goalHeading, shootFreekick, goalFreekick, shootInPenalty, goalInPenalty, shootOutPenalty, goalOutPenalty, shootPenaltyKick, goalPenaltyKick)\
            FROM 's3://fc-online-match/shoot.csv'\
            credentials=(AWS_KEY_ID='{os.environ.get('EC2_ACCESS_KEY')}' AWS_SECRET_KEY='{os.environ.get('EC2_SECRET_KEY')}')\
            FILE_FORMAT = (type='CSV' skip_header=1);\
            "
        cur.execute(copy_command)

        copy_command = f"\
            COPY INTO fc_online.raw_data.pass (matchId, passTry, passSuccess, shortPassTry, shortPassSuccess, longPassTry, longPassSuccess, bouncingLobPassTry, bouncingLobPassSuccess, drivenGroundPassTry, drivenGroundPassSuccess, throughPassTry, throughPassSuccess, lobbedThroughPassTry, lobbedThroughPassSuccess)\
            FROM 's3://fc-online-match/pass.csv'\
            credentials=(AWS_KEY_ID='{os.environ.get('EC2_ACCESS_KEY')}' AWS_SECRET_KEY='{os.environ.get('EC2_SECRET_KEY')}')\
            FILE_FORMAT = (type='CSV' skip_header=1);\
            "
        cur.execute(copy_command)

        copy_command = f"\
            COPY INTO fc_online.raw_data.defence (matchId, blockTry, blockSuccess, tackleTry, tackleSuccess)\
            FROM 's3://fc-online-match/defence.csv'\
            credentials=(AWS_KEY_ID='{os.environ.get('EC2_ACCESS_KEY')}' AWS_SECRET_KEY='{os.environ.get('EC2_SECRET_KEY')}')\
            FILE_FORMAT = (type='CSV' skip_header=1);\
            "
        cur.execute(copy_command)

        cur.execute("COMMIT")
    except Exception as e:
        cur.execute("ROLLBACK")
        print(f"Error: {e}")
    finally:
        cur.close()
        conn.close()

def remove_duplicate():
    conn = connect_snowflake()
    cur = conn.cursor()
    try:
        cur.execute("BEGIN")

        duplicate_commands = [
            'CREATE OR REPLACE TABLE fc_online.raw_data.match_detail AS SELECT DISTINCT * FROM fc_online.raw_data.match_detail;',
            'CREATE OR REPLACE TABLE fc_online.raw_data.shoot AS SELECT DISTINCT * FROM fc_online.raw_data.shoot;',
            'CREATE OR REPLACE TABLE fc_online.raw_data.pass AS SELECT DISTINCT * FROM fc_online.raw_data.pass;',
            'CREATE OR REPLACE TABLE fc_online.raw_data.defence AS SELECT DISTINCT * FROM fc_online.raw_data.defence;'
        ]
        for cmd in duplicate_commands:
            cur.execute(cmd)

        cur.execute("COMMIT")
    except Exception as e:
        cur.execute("ROLLBACK")
        print(f"Error: {e}")
    finally:
        cur.close()
        conn.close()

def create_analytics_tables():
    conn = connect_snowflake()
    cur = conn.cursor()
    try:
        cur.execute("BEGIN")

        create_table_command = f"\
            CREATE OR REPLACE TABLE fc_online.analytics.matchResult_with_match_detail AS\
            SELECT\
                matchResult,\
                COUNT(CASE WHEN controller = 'gamepad' THEN 1 END) AS gamepad_count,\
                COUNT(CASE WHEN controller = 'keyboard' THEN 1 END) AS keyboard_count,\
                SUM (yellowCards) AS total_yellow_cards,\
                SUM (redCards) AS total_red_cards,\
                SUM (foul) AS total_foul,\
                ROUND(AVG (dribble), 1) AS avg_dribble,\
                ROUND(AVG (possession), 1) AS avg_possession\
            FROM fc_online.raw_data.match_detail\
            GROUP BY matchResult;\
            "
        cur.execute(create_table_command)

        create_table_command = f"\
            CREATE OR REPLACE TABLE fc_online.analytics.matchResult_with_shoot AS\
            SELECT\
                matchResult,\
                SUM (effectiveshoottotal) AS total_effective_shoot_total,\
                SUM (goaltotal) AS total_goal_total,\
                SUM (shootpenaltykick) AS total_shoot_penalty_kick\
            FROM fc_online.raw_data.shoot A\
            LEFT JOIN fc_online.raw_data.match_detail B ON A.id = B.id\
            GROUP BY B.matchresult;\
            "
        cur.execute(create_table_command)

        create_table_command = f"\
            CREATE OR REPLACE TABLE fc_online.analytics.matchResult_with_pass AS\
            SELECT\
                matchResult,\
                ROUND(AVG (passTry), 1) AS avg_pass_try,\
                ROUND(AVG (passSuccess), 1) AS avg_pass_success,\
                ROUND(AVG(CASE WHEN passTry = 0 THEN 0 ELSE (passSuccess * 100) / passTry END), 1) AS avg_pass_success_rate\
            FROM fc_online.raw_data.pass A\
            LEFT JOIN fc_online.raw_data.match_detail B ON A.id = B.id\
            GROUP BY B.matchresult;\
            "
        cur.execute(create_table_command)

        create_table_command = f"\
            CREATE OR REPLACE TABLE fc_online.analytics.matchResult_with_defence AS\
            SELECT\
                matchResult,\
                ROUND(AVG (blocktry), 1) AS avg_block_try,\
                ROUND(AVG (blocksuccess), 1) AS avg_block_success,\
                ROUND(AVG(CASE WHEN blocktry = 0 THEN 0 ELSE (blocksuccess * 100) / blocktry END), 1) AS avg_block_success_rate,\
                ROUND(AVG (tackletry), 1) AS avg_tackle_try,\
                ROUND(AVG (tacklesuccess), 1) AS avg_tackle_success,\
                ROUND(AVG(CASE WHEN tackletry = 0 THEN 0 ELSE (tacklesuccess * 100) / tackletry END), 1) AS avg_tackle_success_rate\
            FROM fc_online.raw_data.defence A\
            LEFT JOIN fc_online.raw_data.match_detail B ON A.id = B.id\
            GROUP BY B.matchresult;\
            "
        cur.execute(create_table_command)

        cur.execute("COMMIT")
    except Exception as e:
        cur.execute("ROLLBACK")
        print(f"Error: {e}")
    finally:
        cur.close()
        conn.close()

if __name__=='__main__':
    print(f'{datetime.now()}: Start COPY')

    try:
        create_raw_data_tables()
        copy_from_s3()
        remove_duplicate()
        create_analytics_tables()
        print("Done COPY")
    except Exception as e:
        print(f"Error occurred: {e}")
        raise