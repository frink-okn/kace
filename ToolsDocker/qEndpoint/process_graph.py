import json
import os
import pprint
from collections import Counter
from itertools import chain
from sys import argv

import networkx as nx
import rdflib
from rdflib import URIRef
from rdflib.namespace import Namespace, DCTERMS, GEO, OWL, RDF, PROV, SDO, SKOS, RDFS, XSD

# Just some namespace definitions I've come across, in addition to some of the imported ones above
DREAMKG = Namespace('http://www.semanticweb.org/dreamkg/ijcai/')
SCALES = Namespace("http://schemas.scales-okn.org/rdf/scales#")
SDOH = Namespace("http://schema.org/")
NEO4J = Namespace("neo4j://graph.schema#")
SOCKG = Namespace("http://www.semanticweb.org/zzy/ontologies/2024/0/soil-carbon-ontology/")
SECURECHAIN = Namespace("http://example.org/ns#")
IO = Namespace("https://spec.industrialontologies.org/ontology/core/Core/")
IOSC = Namespace("https://spec.industrialontologies.org/ontology/supplychain/SupplyChain/")
SUDOKN = Namespace("http://asu.edu/semantics/SUDOKN/")
USFRS = Namespace("http://sawgraph.spatialai.org/v1/us-frs#")
USFRSDATA = Namespace("http://sawgraph.spatialai.org/v1/us-frs-data#")
NAICS = Namespace("http://sawgraph.spatialai.org/v1/fio/naics#")
OBO = Namespace("http://purl.obolibrary.org/obo/")
SUDOKN2 = Namespace("Utilities:communication/")
SUDOKN3 = Namespace("Utilities:water/")
RURAL = Namespace("http://sail.ua.edu/ruralkg/")

def get_graph(graph_to_read):
    """ Reads in a set of TTL files from a folder.
        May be modified as needed to crawl files in other places.
    """
    g = rdflib.Graph()

    for root, _, files in os.walk(graph_to_read):
        for name in files:
            current_file_path = os.path.join(root, name)
            if current_file_path.endswith('.ttl') or current_file_path.endswith('.rdf') or current_file_path.endswith('_RDF'):
                if not any(current_file_path.endswith(thing) for thing in ['_ontology.ttl', '-prov.ttl', '-schema.ttl', '-frs.ttl']):
                    g.parse(current_file_path, format='ttl')
    return g

replacements = list(zip(
    [SOCKG, DREAMKG, DCTERMS, XSD, GEO, OBO, OWL, RDF, SCALES, SDOH, SDO, NEO4J, RURAL, SECURECHAIN, PROV, IO, IOSC, SUDOKN, SUDOKN2, SUDOKN3, USFRSDATA, USFRS, NAICS, SKOS, RDFS],
    ['sockg:', 'dreamkg:', 'dct:', 'xsd:', 'geo:', 'obo:', 'owl:', 'rdf:', 'scales:', 'sdoh:', 'schema:', 'neo4j:', 'rural:', 'securechain:', 'prov:', 'io:', 'iosc:', 'sudokn:', 'sudokn2:', 'sudokn3:', 'usfrsdata:', 'usfrs:', 'naics:', 'skos:', 'rdfs:']
))
""" Just some substitutions of the namespaces above with prefixes for ease of reading. """

replaced_prefixes = set()

def replace_prefixes(node):
    """ Replaces a URI prefix with the abbreviation as given in 'replacements' above. """
    for prefix, replacement in replacements:
        removed = node.removeprefix(str(prefix))
        if removed != str(node):
            removed = replacement + removed
            node = removed
            replaced_prefixes.add((prefix, replacement))
    return node

def iter_triples(g, subject=None, predicate=None, object_in=None):
    """ Convenience function returning an iterator across triples. """
    return g.triples((subject, predicate, object_in))

def all_triples_type(g, subject_type, predicate=None, object_in=None):
    """ Convenience function returning an iterator across triples
        where the subject of the triple has a certain rdf:type
        and where each URI is run through replace_prefixes() above.
    """
    for (subject, _, _) in g.triples((None, RDF.type, subject_type)):
        for x, y, z in g.triples((subject, predicate, object_in)):
            print([replace_prefixes(x), replace_prefixes(y), replace_prefixes(z)])

def all_triples(g, subject=None, predicate=None, object_in=None):
    """ Convenience function returning an iterator across triples
        where each URI is run through replace_prefixes() above.
    """
    for (x, y, z) in iter_triples(g, subject, predicate, object_in):
        print([replace_prefixes(x), replace_prefixes(y), replace_prefixes(z)])



if __name__ == '__main__':
    # Reads the graphs...
    graph_to_read = argv[1]
    output_dir = argv[2]
    g = get_graph(graph_to_read)

    # ...sets up the counters...
    subject_types = Counter()
    type_predicate_type_pairs = Counter()
    type_predicate_None_pairs = Counter()
    None_predicate_type_pairs = Counter()
    None_predicates = Counter()
    entities_without_type = set()

    # ...and starts counting!
    for subj, pred, obj in g:
        if pred == RDF.type:
            subject_types.update([obj])
        else:
            subject_type = g.value(subject=subj, predicate=RDF.type)
            object_type = g.value(subject=obj, predicate=RDF.type)
            if subject_type is not None and object_type is not None:
                type_predicate_type_pairs.update([(subject_type, pred, object_type)])
            elif subject_type is not None and object_type is None:
                if isinstance(obj, URIRef):
                    object_datatype = XSD.anyURI
                else:
                    object_datatype = obj.datatype
                if object_datatype is None:
                    object_datatype = '(untyped)'
                type_predicate_None_pairs.update([(subject_type, pred, object_datatype)])
            elif subject_type is None:
                entities_without_type.add(subj)
                if object_type is not None:
                    None_predicate_type_pairs.update([(pred, object_type)])
                else:
                    if isinstance(obj, URIRef):
                        object_datatype = XSD.anyURI
                    else:
                        object_datatype = obj.datatype
                    if object_datatype is None:
                        object_datatype = '(untyped)'
                    None_predicates.update([(pred, object_datatype)])

    # Assembles the JSON output...
    stats = {}
    stats['type_counts'] = [
        {'name': replace_prefixes(typename), 'count': value, 'comment': ""}
        for typename, value in subject_types.most_common()
    ]
    stats['type_predicate_type_counts'] = [
        {'setup': {'subject': replace_prefixes(subject), 'predicate': replace_prefixes(predicate), 'object': replace_prefixes(obj)}, 'count': value, 'comment': ""}
        for (subject, predicate, obj), value in type_predicate_type_pairs.most_common()
    ]
    stats['type_predicate_None_counts'] = [
        {'setup': {'subject': replace_prefixes(subject), 'predicate': replace_prefixes(predicate), 'object_datatype': replace_prefixes(object_datatype)}, 'count': value, 'comment': ""}
        for (subject, predicate, object_datatype), value in type_predicate_None_pairs.most_common()
    ]
    stats['None_predicate_type_counts'] = [
        {'setup': {'predicate': replace_prefixes(predicate), 'object': replace_prefixes(obj)}, 'count': value, 'comment': ""}
        for (predicate, obj), value in None_predicate_type_pairs.most_common()
    ]
    stats['None_predicate_None_counts'] = [
        {'setup': {'predicate': replace_prefixes(predicate), 'object_datatype': replace_prefixes(object_datatype)}, 'count': value, 'comment': ""}
        for (predicate, object_datatype), value in None_predicates.most_common()
    ]
    stats['entities_without_type'] = [replace_prefixes(entity) for entity in entities_without_type]
    stats['prefixes'] = [{'prefix': prefix, 'definition': str(ns)} for ns, prefix in replaced_prefixes]

    # ...and dumps it!
    report_file_name = output_dir.rstrip('/') + '/graph_stats.json'
    with open(report_file_name,'w') as f:
        json.dump(stats, f, indent=2)

    # (The remainder produces a simple Graphviz graph of the links between typed entities.)
    G = nx.Graph()

    for (s, p, o), l in type_predicate_type_pairs.most_common():
        s = "\"" + replace_prefixes(str(s)) + "\""
        p = replace_prefixes(str(p))
        o = "\"" + replace_prefixes(str(o)) + "\""
        # skip since these were just too frequent in SUDOKN to make the graph readable
        if p in ['"sudokn:hasProcessCapability"', '"sudokn:hasMaterialCapability"']:
            continue
        G.add_edge(s, o, dir='forward', arrowhead='normal', label='"' + str(l) + 'x ' + p + '"')
    graph_dot_file_name = output_dir.rstrip('/') + '/schema.dot'
    nx.drawing.nx_pydot.write_dot(G, graph_dot_file_name)