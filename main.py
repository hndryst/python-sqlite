import sqlite3
import pandas
from contextlib import closing


def openConnection():
    with closing(sqlite3.connect("resourceDiscovery.db")) as connection:
        with closing(connection.cursor()) as cursor:
            # importProjectList(connection)
            importBillingData(connection)

            # rows = cursor.execute(query).fetchall()
            # print(rows)

            with open("output/output.csv", "w") as file:
                pass

            queries = [
                'SELECT DISTINCT projectID,"computeEngine" as module FROM billing WHERE service = "Compute Engine" AND skuDesc like "%Instance Core running%"',
                'SELECT DISTINCT projectID,"cloudRun" as module FROM billing WHERE service = "Cloud Run"',
                'SELECT DISTINCT projectID,"GKE" as module FROM billing WHERE service = "Kubernetes Engine"',
                'SELECT DISTINCT projectID,"cloudStorage" as module FROM billing WHERE service = "Cloud Storage"',
                'SELECT DISTINCT projectID,"cloudFilestore" as module FROM billing WHERE service = "Cloud Filestore"',
                'SELECT DISTINCT projectID,"cloudSQL" as module FROM billing WHERE service = "Cloud SQL"',
                'SELECT DISTINCT projectID,"cloudDataflow" as module FROM billing WHERE service = "Cloud Dataflow"',
                'SELECT DISTINCT projectID,"cloudDataproc" as module FROM billing WHERE service = "Compute Engine" AND skuDesc like "%Dataproc%"',
                'SELECT DISTINCT projectID,"cloudDataFusion" as module FROM billing WHERE service = "Cloud Data Fusion"',
                'SELECT DISTINCT projectID,"cloudPubSub" as module FROM billing WHERE service = "Cloud Pub/Sub"',
                'SELECT DISTINCT projectID,"cloudDNS" as module FROM billing WHERE service = "Cloud DNS"',
                'SELECT DISTINCT projectID,"cloudLB" as module FROM billing WHERE service = "Compute Engine" AND skuDesc like "%Load Balancing%"',
                'SELECT DISTINCT projectID,"cloudNAT" as module FROM billing WHERE service = "Networking" AND skuDesc like "%Cloud Nat%"',
                'SELECT DISTINCT projectID,"cloudVPN" as module FROM billing WHERE service = "Networking" AND skuDesc like "%Cloud VPN Tunnel%"',
                'SELECT DISTINCT projectID,"cloudInterconnect" as module FROM billing WHERE service = "Compute Engine" AND skuDesc like "%Cloud Interconnect%"',
                'SELECT DISTINCT projectID,"cloudMemorystore" as module FROM billing WHERE service = "Cloud Memorystore for Redis" OR service = "Cloud Memorystore for Memcached"',
            ]

            for query in queries:
                df = pandas.read_sql(query, connection)
                df.to_csv("output/output.csv", index=False, mode="a", header=False)


# def importProjectList(connection):
#     data = pandas.read_csv("input/project.txt")
#     data.to_sql("project", connection, if_exists="replace", chunksize=64)


def importBillingData(connection):
    data = pandas.read_csv("input/billing.csv")
    data.to_sql("billing", connection, if_exists="replace", chunksize=64)


def main():
    openConnection()


if __name__ == "__main__":
    main()
