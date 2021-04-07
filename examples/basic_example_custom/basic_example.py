# -*- coding: utf-8 -*-
import os

from kikimr.public.sdk.python import client as ydb
from kikimr.public.sdk.python.client.issues import Aborted
from concurrent.futures import TimeoutError
import basic_example_data
from clj_l import loads
import edn_format
import time


StartAppendTransactionsBlock =\
    """ 
    DECLARE $data AS "List<Struct<key:Uint64, value:Utf8>>";
    """


FillDataQuery = """PRAGMA TablePathPrefix("{}");

DECLARE $seriesData AS List<Struct<
    series_id: Uint64,
    title: Utf8,
    series_info: Utf8,
    release_date: Date>>;

DECLARE $seasonsData AS List<Struct<
    series_id: Uint64,
    season_id: Uint64,
    title: Utf8,
    first_aired: Date,
    last_aired: Date>>;

DECLARE $episodesData AS List<Struct<
    series_id: Uint64,
    season_id: Uint64,
    episode_id: Uint64,
    title: Utf8,
    air_date: Date>>;

REPLACE INTO series
SELECT
    series_id,
    title,
    series_info,
    CAST(release_date AS Uint64) AS release_date
FROM AS_TABLE($seriesData);

REPLACE INTO seasons
SELECT
    series_id,
    season_id,
    title,
    CAST(first_aired AS Uint64) AS first_aired,
    CAST(last_aired AS Uint64) AS last_aired
FROM AS_TABLE($seasonsData);

REPLACE INTO episodes
SELECT
    series_id,
    season_id,
    episode_id,
    title,
    CAST(air_date AS Uint64) AS air_date
FROM AS_TABLE($episodesData);
"""


def describe_table(session, path, name):
    result = session.describe_table(os.path.join(path, name))
    print("\n> describe table: registers")
    for column in result.columns:
        print("column, name:", column.name, ",", str(column.type.item).strip())


def bulk_upsert(table_client, path):
    print("\n> bulk upsert: episodes")
    column_types = ydb.BulkUpsertColumns() \
        .add_column('series_id', ydb.OptionalType(ydb.PrimitiveType.Uint64)) \
        .add_column('season_id', ydb.OptionalType(ydb.PrimitiveType.Uint64)) \
        .add_column('episode_id', ydb.OptionalType(ydb.PrimitiveType.Uint64)) \
        .add_column('title', ydb.OptionalType(ydb.PrimitiveType.Utf8)) \
        .add_column('air_date', ydb.OptionalType(ydb.PrimitiveType.Uint64))
    rows = basic_example_data.get_episodes_data_for_bulk_upsert()
    table_client.bulk_upsert(os.path.join(path, "episodes"), rows, column_types)


def is_directory_exists(driver, path):
    try:
        return driver.scheme_client.describe_path(path).is_directory()
    except ydb.SchemeError:
        return False


def ensure_path_exists(driver, database, path):
    paths_to_create = list()
    path = path.rstrip("/")
    while path not in ("", database):
        full_path = os.path.join(database, path)
        if is_directory_exists(driver, full_path):
            break
        paths_to_create.append(full_path)
        path = os.path.dirname(path).rstrip("/")

    while len(paths_to_create) > 0:
        full_path = paths_to_create.pop(-1)
        driver.scheme_client.make_directory(full_path)


def fill_tables_with_data(session, path):
    global FillDataQuery

    prepared_query = session.prepare(FillDataQuery.format(path))
    session.transaction(ydb.SerializableReadWrite()).execute(
        prepared_query, {
            '$seriesData': basic_example_data.get_series_data(),
            '$seasonsData': basic_example_data.get_seasons_data(),
            '$episodesData': basic_example_data.get_episodes_data(),
        },
        commit_tx=True,
    )


def select_prepared(session, path, series_id, season_id, episode_id):
    query = """
    PRAGMA TablePathPrefix("{}");

    DECLARE $seriesId AS Uint64;
    DECLARE $seasonId AS Uint64;
    DECLARE $episodeId AS Uint64;

    $format = DateTime::Format("%Y-%m-%d");
    SELECT
        title,
        $format(DateTime::FromSeconds(CAST(DateTime::ToSeconds(DateTime::IntervalFromDays(CAST(air_date AS Int16))) AS Uint32))) AS air_date
    FROM episodes
    WHERE series_id = $seriesId AND season_id = $seasonId AND episode_id = $episodeId;
    """.format(path)

    prepared_query = session.prepare(query)
    result_sets = session.transaction(ydb.SerializableReadWrite()).execute(
        prepared_query, {
            '$seriesId': series_id,
            '$seasonId': season_id,
            '$episodeId': episode_id,
        },
        commit_tx=True
    )
    print("\n> select_prepared_transaction:")
    for row in result_sets[0].rows:
        print("episode title:", row.title, ", air date:", row.air_date)

    return result_sets[0]


# Show usage of explicit Begin/Commit transaction control calls.
# In most cases it's better to use transaction control settings in session.transaction
# calls instead to avoid additional hops to YDB cluster and allow more efficient
# execution of queries.
def explicit_tcl(session, path, series_id, season_id, episode_id):
    query = """
    PRAGMA TablePathPrefix("{}");

    DECLARE $seriesId AS Uint64;
    DECLARE $seasonId AS Uint64;
    DECLARE $episodeId AS Uint64;

    UPDATE episodes
    SET air_date = CAST(CurrentUtcDate() AS Uint64)
    WHERE series_id = $seriesId AND season_id = $seasonId AND episode_id = $episodeId;
    """.format(path)
    prepared_query = session.prepare(query)

    # Get newly created transaction id
    tx = session.transaction(ydb.SerializableReadWrite()).begin()

    # Execute data query.
    # Transaction control settings continues active transaction (tx)
    tx.execute(
        prepared_query, {
            '$seriesId': series_id,
            '$seasonId': season_id,
            '$episodeId': episode_id
        }
    )

    print("\n> explicit TCL call")

    # Commit active transaction(tx)
    tx.commit()


def read(session, path, register_id):
    # new transaction in serializable read write mode
    # if query successfully completed you will get result sets.
    # otherwise exception will be raised
    result_sets = session.transaction(ydb.SerializableReadWrite()).execute(
        """
        PRAGMA TablePathPrefix("{}");
        SELECT
            register_id,
            value
        FROM registers
        WHERE register_id = {};
        """.format(path, register_id),
        commit_tx=True,
    )
    print("\n> read transaction:")
    for row in result_sets[0].rows:
        print("register, id: ", row.register_id, ", value: ", row.value)

    return result_sets[0]

"""
PRAGMA TablePathPrefix("{}");
UPDATE registers
SET value = CAST(value ||  "1" As Utf8)
WHERE
    register_id = {}
;
"""


def upsert(session, path, register_id, value):
    session.transaction().execute(
        """
        PRAGMA TablePathPrefix("{}");
        UPDATE registers
        SET value = CAST(value ||  " ,{}" As Utf8)
        WHERE
            register_id = {}
        ;
        """.format(path, value, register_id),
        commit_tx=True,
    )

    print("\n> append transaction:")
    print("register, id: ", register_id, ", value: ", value)


def insert_preparation(session, path, number_of_keys):
    print("\n> Prepare table: ...")
    query = """
            PRAGMA TablePathPrefix("{}");
            UPSERT INTO registers
            (
                register_id
            )
            VALUES
            """.format(path)

    for i in range(1, number_of_keys + 1):
        if i == number_of_keys:
            query += """
                    (
                        {}
                    );
                    """.format(i)
            continue
        if i % 90 == 0:
            query += """
                     (
                        {}
                     );
                     """.format(i)
            try:
                session.transaction().execute(
                    query,
                    commit_tx=True,
                )
            except ydb.issues.Overloaded:
                time.sleep(0.1)
                session.transaction().execute(
                    query,
                    commit_tx=True,
                )

            query = """
            PRAGMA TablePathPrefix("{}");
            UPSERT INTO registers
            (
                register_id
            )
            VALUES
            """.format(path)
            continue
        query += """
                 (
                    {}
                 ),
                 """.format(i)
    try:
        session.transaction().execute(
            query,
            commit_tx=True,
        )
    except ydb.issues.Overloaded:
        time.sleep(0.1)
        session.transaction().execute(
            query,
            commit_tx=True,
        )


def create_tables(session, path):
    # Creating registers table
    print(session.drop_table(os.path.join(path, 'registers')))
    print(session.create_table(
        os.path.join(path, 'registers'),
        ydb.TableDescription()
        .with_column(ydb.Column('register_id', ydb.OptionalType(ydb.PrimitiveType.Uint64)))
        .with_column(ydb.Column('value', ydb.OptionalType(ydb.PrimitiveType.Utf8)))
        .with_primary_key('register_id')
    ))


def get_read_query(register_id):
    return """
    SELECT
        register_id,
        value
    FROM registers
    WHERE register_id = {};
    """.format(register_id)


def get_append_query(path, register_id, value):
    return """
    PRAGMA TablePathPrefix("{}");
    UPDATE registers
    SET value = CAST(value ||  " ,{}" As Utf8)
    WHERE
        register_id = {}
    ;
    """.format(path, value, register_id)


def _run_transaction(session, query, query_arguments):
    # new transaction in serializable read write mode
    # if query successfully completed you will get result sets.
    # otherwise exception will be raised
    prepared_query = session.prepare(query)
    # Get newly created transaction id
    tx = session.transaction(ydb.SerializableReadWrite()).begin()

    # Execute data query.
    # Transaction control settings continues active transaction (tx)
    result_sets = tx.execute(
        prepared_query, {
            '$data': query_arguments
        },
    )
    tx.commit()

    print("\n> Results:")
    for index, result in enumerate(result_sets):
        print("\n> Result #{}:".format(index))
        for row in result.rows:
            print("register, id: ", row.register_id, ", value: ", row.value)

    return result_sets


def run_transaction(session, query, query_arguments):
    try:
        return _run_transaction(session, query, query_arguments)
    except ydb.issues.Overloaded:
        time.sleep(0.2)
        return _run_transaction(session, query, query_arguments)
    except ydb.issues.NotFound:
        return _run_transaction(session, query, query_arguments)


def get_query_by_command(path, command):
    if command[0] == 'append':
        print("\n   > append transaction:")
        print("     register, id: ", command[1], ", value: ", command[2])
        return get_append_query(path, command[1], command[2])
    if command[0] == 'r':
        print("\n   > read transaction:")
        return get_read_query(path, command[1])


def get_query_from_elle_dict(path, transaction):
    """
    :param transaction: transaction dict in Elle format
    :param path: a path to table we work with
    :return: query, arguments for query: python string, python dict
    """

    query = """PRAGMA TablePathPrefix("{}");""".format(path)
    has_append = False
    query_arguments = []
    print(type(transaction))
    for command in transaction['value']:
        if command[0] == 'append':
            query +=\
                """
                DECLARE $data AS List<Struct<register_id:Uint64, value:Utf8>>;
                """
            break
    for command in sorted(transaction['value'], reverse=True):
        if command[0] == 'r':
            query += """PRAGMA TablePathPrefix("{}");""".format(path)
            query += get_read_query(command[1])
            continue

        if command[0] == 'append' and not has_append:
            has_append = True
            query += """PRAGMA TablePathPrefix("{}");""".format(path)
            query +=\
                """
                $result = (SELECT d.register_id AS register_id, t.value || d.value AS value FROM AS_TABLE($data)
                 AS d LEFT JOIN registers AS t ON t.register_id = d.register_id);
                UPSERT INTO registers SELECT register_id, value FROM $result;
                """
            query_arguments.append({'register_id': command[1],
                                    'value': ',' + str(command[2])
                                    })
            continue

        if command[0] == 'append' and has_append:
            query_arguments.append({'register_id': command[1],
                                    'value': ',' + str(command[2])
                                    })
            continue
    return query, query_arguments


def fix_output(output):
    return output.replace('"type" "ok"', ':type :ok,')\
        .replace('"f" "txn" ', ' :f :txn ,')\
        .replace('"value"', ':value')\
        .replace('"r"', ':r')\
        .replace('"append"', ':append')\
        .replace('[nil]', 'nil')


def transform(x):
    if x == 'None':
        return None
    else:
        return int(x)


def divide_transactions_in_batches(transactions, batches_number):
    """
    :param transactions: array of jsons
    example: [{'type': 'invoke', 'f': 'txn', 'value': [['r', 49, None], ['append', 47, 1], ['append', 49, 1], ['r', 48, None], ['r', 49, None], ['append', 45, 1]]}, {'type': 'invoke', 'f': 'txn', 'value': [['r', 46, None], ['append', 49, 2], ['r', 49, None], ['r', 48, None], ['r', 43, None]]}]
    :param batches_number: number of batches array to break in
    :return: array of transactions (array of arrays of jsons)
    """
    avg = len(transactions) / float(batches_number)
    batches = []
    last = 0.0

    while last < len(transactions):
        batches.append(transactions[int(last):int(last + avg)])
        last += avg

    return batches


def get_transactions_by_path(path):
    """
    :param path: path to transactions in ELLE format
    :return: return content of the file
    """
    with open(path, "r") as txns_file:
        """.replace('Keyword(value)', 'value')\
        .replace('Keyword(invoke)', 'invoke')\
        .replace('Keyword(f)', 'f')\
        .replace('Keyword(type)', 'type')\
        .replace('Keyword(txn)', 'txn')\
        .replace('Keyword(r)', 'r')\
        """
        return loads(txns_file.read())


def run_transactions_batch(full_path, session, transactions_batch):
    """
    :param full_path: full path to DB
    :param session: DB session
    :param transactions_batch: array of jsons representing transaction
    :return: return ready transactions set filled with returned values
    """
    for index, transaction in enumerate(transactions_batch):
        print("\n> Start new transaction:")
        query, query_arguments = get_query_from_elle_dict(full_path, transaction)
        print("\n>     Query body:"
              "\n      {}".format(query))
        print("\n>     Query arguments:"
              "\n      {}".format(query_arguments))
        while True:
            try:
                result_sets = run_transaction(session, query, query_arguments)
            except Aborted:
                continue
            else:
                break

        if result_sets is not None:
            for jndex, command in enumerate(sorted(transaction['value'], reverse=True)):
                if command[0] == 'append':
                    break
                else:
                    for row in result_sets[jndex].rows:
                        x = str(row.value).split(",")
                        command[2] = list(map(transform, x))
            transaction['type'] = 'ok'
    return transactions_batch


def dump_transactions_batch(filepath, transactions_batch):
    """
    :param filepath: path to save transactions file
    :param transactions_batch: ready transactions
    :return: just write transaction to disk formatted in Elle way
    """
    with open(filepath + '-ready', "w") as txns_output_file:
        for transaction in transactions_batch:
            transaction['value'] = sorted(transaction['value'], reverse=True)
        txns_output_file.writelines(fix_output(edn_format.dumps(transactions_batch)))


def run(endpoint, database, path):
    driver_config = ydb.DriverConfig(
        endpoint, database, credentials=ydb.construct_credentials_from_environ(),
        root_certificates=ydb.load_ydb_root_certificate(),
    )
    with ydb.Driver(driver_config) as driver:
        try:
            driver.wait(timeout=5)
        except TimeoutError:
            print("Connect failed to YDB")
            print("Last reported errors by discovery:")
            print(driver.discovery_debug_details())
            exit(1)

        session = driver.table_client.session().create()
        ensure_path_exists(driver, database, path)

        full_path = os.path.join(database, path)

        #create_tables(session, full_path)
        describe_table(session, full_path, "registers")

        insert_preparation(session, full_path, 1000)

        txns_path = "results/txns-short"

        transactions = get_transactions_by_path(txns_path)
        transactions_batches = divide_transactions_in_batches(transactions, 5)
        for batch_number, batch in enumerate(transactions_batches):
            proccessed_batch = run_transactions_batch(full_path, session, batch)
            dump_transactions_batch(txns_path + str(batch_number), proccessed_batch)



        """
        with open(txns_path, "r") as txns_file:
            transactions = clj.loads(txns_file.read())
            for index, transaction in enumerate(transactions):
                print("\n> Start new transaction:")
                query, query_arguments = get_query_from_elle_dict(full_path, transaction)
                print("\n>     Query body:"
                      "\n      {}".format(query))
                print("\n>     Query arguments:"
                      "\n      {}".format(query_arguments))
                result_sets = run_transaction(session, query, query_arguments)
                if result_sets is not None:
                    for jndex, command in enumerate(sorted(transaction['value'], reverse=True)):
                        if command[0] == 'append':
                            break
                        else:
                            for row in result_sets[jndex].rows:
                                x = str(row.value).split(",")
                                command[2] = list(map(transform, x))
                    transaction['type'] = 'ok'
            with open(txns_path + '-ready', "w") as txns_output_file:
                for transaction in transactions:
                    transaction['value'] = sorted(transaction['value'], reverse=True)
                print(fix_output(edn_format.dumps(transactions)))
                txns_output_file.writelines(fix_output(edn_format.dumps(transactions)))

        """

        #upsert_simple(session, full_path)

        #bulk_upsert(driver.table_client, full_path)

        #select_prepared(session, full_path, 2, 3, 7)
        #select_prepared(session, full_path, 2, 3, 8)

        #explicit_tcl(session, full_path, 2, 6, 1)
        #select_prepared(session, full_path, 2, 6, 1)
