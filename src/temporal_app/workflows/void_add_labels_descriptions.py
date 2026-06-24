import argparse
import pathlib
from itertools import batched
from sys import argv

from rdflib import Graph, Namespace
from rdflib.namespace import DC, DCTERMS, PROV, RDF, RDFS, SDO, SKOS, VOID
from rdflib_hdt import HDTDocument

SDOH = Namespace("http://schema.org/")
VOIDEXT = Namespace("http://ldf.fi/void-ext#")
FEDERATION_URL = "https://frink.apps.renci.org/federation/sparql"

def add_labels_descriptions(args):
    """Retrieves labels and descriptions for entities.

    First the graph itself is queried for these things,
    and then the federated endpoint is re-queried for them.
    """

    document = HDTDocument(str(args.hdt_file))

    void_graph = rdflib.Graph()
    void_graph.parse(args.input, format="nt")

    entities_to_check = set()

    dataset_iri = next(obj for obj in void_graph.subjects(RDF.type, VOID.Dataset))

    for partition_type, partition_predicate in [
        (VOID.classPartition, VOID["class"]),
        (VOID.propertyPartition, VOID.property),
        (VOIDEXT.objectClassPartition, VOID["class"])
    ]:
        for partition in void_graph.subjects(RDF.type, partition_type):
            for target in void_graph.objects(partition, partition_predicate):
                entities_to_check.add('<' + str(target) + '>')

    for pred, pred_options in [
        [RDFS.label, [RDFS.label, SDO.name, SDOH.name, DCTERMS.title, DC.title,],],
        [SKOS.definition, [DCTERMS.description, DC.description, SKOS.definition, SDO.description, SDOH.description, PROV.definition, RDFS.comment,],],
    ]:
        for pred_to_try in pred_options:
            triples, cardinality = document.search((None, pred_to_try, None))
            for s, p, o in triples:
                if s in entities_to_check:
                    void_graph.add((s, pred, o))

        for batch in batched(list(entities_to_check), 50):
            target_query = f"""
SELECT ?s ?p ?o
WHERE {{
SERVICE <{FEDERATION_URL}> {{
    values ?s {{ {" ".join(batch)} }}
    values ?p {{ {" ".join([('<' + str(uri) + '>') for uri in pred_options])} }}
    ?s ?p ?o .
}}
}}
"""
            qres = void_graph.query(target_query)
            for row in qres:
                void_graph.add((getattr(row, 's'), getattr(row, 'p'), getattr(row, 'o')))

    void_graph.serialize(destination=args.output, format="turtle")

parser = argparse.ArgumentParser()
parser.add_argument(
    'hdt_file',
    type=pathlib.Path
)
parser.add_argument(
    '-i',
    '--input',
    required=True,
    type=pathlib.Path,
    help="Input file path for original VoID description (Turtle format)"
)
parser.add_argument(
    '-o',
    '--output',
    required=True,
    type=pathlib.Path,
    help="Output file path for modified VoID description (Turtle format)"
)

if __name__ == '__main__':
    args = parser.parse_args()
    add_labels_descriptions(args)