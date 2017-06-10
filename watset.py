
#######################################################
###   Utils and imports
#######################################################
import math
import random
from collections import defaultdict
from pyspark.sql import Row

def argmax(items, default_value = 0.0):
    best_arg, best_value = None, default_value
    for arg, value in items:
        if value > best_value:
            best_arg, best_value = arg, value
    return best_arg

#######################################################
###   Chinise Whispers (graph clustering algorithm)
#######################################################

def get_major_class(class_weights):
    cnt = defaultdict(float)
    for cls, weight in class_weights:
        cnt[cls] += weight

    return argmax(cnt.items(), default_value=-1.0)

##### Local variant

def update_classes(V, reb, classes):
    random.shuffle(V)
    changed = False

    for u in V:
        neighbor_classes = ( (classes[v], w) for v, w in reb[u])
        new_class = get_major_class(neighbor_classes)
        if new_class != classes[u]:
            classes[u] = new_class
            changed = True
    
    return changed

def make_clusters_from_classes(classes):
    clusters = defaultdict(list)
    for u, cls in classes.items():
        clusters[cls].append(u)

    return list(clusters.values())

def reweight_edges(reb, weights_mode='DIST_LOG'):    
    if weights_mode == 'DIST_LOG':
        calc_w = lambda v, w: w / math.log(len(reb[v]) + 1)
    elif weights_mode == 'DIST_NOLOG':
        calc_w = lambda v, w: w / len(reb[v])
    else:
        raise ValueError('Invalid weights_mode = {!r}: should be DIST_LOG or DIST_NOLOG'.format(weights_mode))
    
    new_reb = {}
    for u in reb.keys():
        new_reb[u] = [ (v, calc_w(v, w)) for v, w in reb[u] ]
    return new_reb

# weights_mode = DIST_LOG, DIST_NOLOG, TOP
# reb format: reb[u] = [ (edge1_v, edge1_weight), ... , (edgeN_v, edgeN_weight) ]
def chinise_whispers(reb, max_steps = 10, weights_mode='TOP'):
    if len(reb) == 1: #optimization shortcut
        return [list(reb.keys())]
    
    if weights_mode != 'TOP':
        reb = reweight_edges(reb, weights_mode)
    
    V = list(reb.keys())
    classes = { u : i for i, u in enumerate(V) }
    
    for i in range(max_steps):
        if not update_classes(V, reb, classes):
            break
    
    return make_clusters_from_classes(classes)

##### Parallel chinise whispers on DataFrames

def parallel_update_classes(classes_df):
    classes_df.createOrReplaceTempView('classes')
    
    neighbor_classes = spark.sql("select e.u, c.cls, e.weight FROM edges e, classes c WHERE e.v=c.v")
    neighbor_classes = neighbor_classes.rdd.map(lambda row: (row[0], (row[1], row[2]))).groupByKey()
    
    return neighbor_classes.map(lambda pair: Row(v=pair[0], cls=get_major_class(pair[1]))).toDF()

def parallel_reweight_edges(edges_df, weights_mode='DIST_LOG'):
    edges_df.createOrReplaceTempView('edges')
    node_degrees = spark.sql('SELECT u as v, count(v) as cnt FROM edges GROUP BY u')
    node_degrees.createOrReplaceTempView('degrees')
    
    if weights_mode == 'DIST_LOG':
        return spark.sql('SELECT e.u, e.v, (e.weight / log(d.cnt + 1)) as weight FROM edges e, degrees d WHERE e.v = d.v')
    elif weights_mode == 'DIST_NOLOG':
        return spark.sql('SELECT e.u, e.v, (e.weight / d.cnt) as weight FROM edges e, degrees d WHERE e.v = d.v')
    else:
        raise ValueError('Invalid weights_mode = {!r}: should be DIST_LOG or DIST_NOLOG'.format(weights_mode))

# edges_df must be Dataframe of edges with colums: u, v, weight
# classes_df must be Dataframe of initial classes with columns: v, cls
# additionally sorts resulting clusters by size from larger to smaller ones
def parallel_chinise_whispers(edges_df, classes_df, max_steps = 10, weights_mode='TOP'):
    if weights_mode != 'TOP':
        edges_df = parallel_reweight_edges(edges_df, weights_mode)
    
    edges_df.createOrReplaceTempView('edges')
    # One possible optimization, but needs more investigation
    ## edges_df = spark.sql('select * FROM edges CLUSTER BY edges.v')
    ## edges_df.createOrReplaceTempView('edges')
    
    for i in range(max_steps):
        classes_df = parallel_update_classes(classes_df).cache()
    
    classes_df.createOrReplaceTempView('classes')
    return spark.sql("SELECT collect_list(c.v) as cluster FROM classes c GROUP BY c.cls ORDER BY size(cluster) DESC")


#######################################################
###   Graph reading and writing
#######################################################

def format_sense(id_to_word, s):
    return id_to_word[s // MAX_SENSES_PER_WORD] + str(s % MAX_SENSES_PER_WORD)

def format_cluster(id_to_word, cluster):
    return '{0}\t{1}'.format(len(cluster), ', '.join(format_sense(id_to_word, s) for s in cluster) )

def format_word(id_to_word, s):
    return id_to_word[s // MAX_SENSES_PER_WORD]

def format_synset(id_to_word, cluster):
    return ', '.join(format_word(id_to_word, s) for s in cluster)

def read_graph_from_spark(path):
    inp = sc.textFile(path).map(lambda t : t.split('\t')).cache()
    vertices = inp.flatMap(lambda t: t[:2]).distinct()
    
    # make word -> word_id and backwards mappings
    indexed_vertices = vertices.zipWithIndex().cache()
    word_to_id = indexed_vertices.collectAsMap()
    id_to_word = indexed_vertices.map(lambda x : (x[1], x[0])).collectAsMap()
    
    # map edges from (str)
    edges_rdd = inp.map(lambda edge: (word_to_id[edge[0]], (word_to_id[edge[1]], float(edge[2])) ) )
    edges = edges_rdd.groupByKey().collectAsMap()
    
    return edges, word_to_id, id_to_word

def save_synsets_to_file(id_to_word, clusters_rdd, path):
    def format_pair(pair):
        cluster, id = pair
        return '{0}\t{1}\t{2}'.format(id, len(cluster), format_synset(id_to_word, cluster))
    
    synsets = clusters_rdd.zipWithIndex().map(format_pair)
    synsets.saveAsTextFile(path)


#######################################################
###   Making EGO graphs
#######################################################
#   Note: diffrence from original: no limit on edges count in EGO subgraph

def filter_edges(edges, V):
    return list(filter(lambda edge: edge[0] in V, edges))

def make_ego(graph_edges, u):
    V = set(map(lambda x: x[0], graph_edges[u]))
    return { v : filter_edges(graph_edges[v], V) for v in V}


#######################################################
###   Converting clustered EGO-graphs to contexts
#######################################################

# Note: This one is important constant, must be big enough
MAX_SENSES_PER_WORD = 1000

def make_contexts(graph_edges, pair):
    u, clusters = pair
    weights = dict(graph_edges[u])
    
    for i, cluster in enumerate(clusters):
        s = u*MAX_SENSES_PER_WORD + i
        context = {v : weights[v] for v in cluster}
        yield u, (s, context)


#######################################################
###   Disambiguate
#######################################################

def cos_sim(ctx1, ctx2):
    dot_product = 0
    
    sum1 = 0
    sum2 = 0
    
    for u, weight in ctx1.items():
        dot_product += weight * ctx2.get(u, 0)
        sum1 += weight**2
        
    for _, weight in ctx2.items():
        sum2 += weight**2
        
    return dot_product / math.sqrt(sum1 * sum2)

def disambiguate_word(word_senses, context):
    similarities = ( (sense_id, cos_sim(context, sense_context)) for sense_id, sense_context in word_senses )
    return argmax(similarities)

def disambiguate_context(all_senses, u, s, current_context):
    context = dict(current_context)
    context[u] = 1 # hack to add current word to context as in disambiguate.py
      
    new_context = [ (disambiguate_word(all_senses[v], context), weight) for v, weight in current_context.items() ]
    return s, new_context


#######################################################
###   Watset
#######################################################

def watset(input_path, output_path, local_cw_params = {}, global_cw_params = {}, use_parallel_cw = False):
    # read input graph, edges[u] = [(edge1_v, edge1_weight), ..., (edgeN_v, edgeN_weight) ]
    edges, word_to_id, id_to_word = read_graph_from_spark(input_path)
    edges = sc.broadcast(edges)
    
    # make ego graphs and cluster them
    ego_graphs = sc.parallelize(edges.value.keys()).map(lambda v: (v, make_ego(edges.value, v)))
    clustered_ego_graphs = ego_graphs.map(lambda pair: (pair[0], chinise_whispers(pair[1], **local_cw_params)))

    # make contexts from clustered graphs (ctx(s) in the paper)
    # senses[s] = ctx(s) = { u_i : weight_i, ... }
    contexts = clustered_ego_graphs.flatMap(lambda pair : make_contexts(edges.value, pair)).cache()
    senses = contexts.groupByKey().collectAsMap()
    senses = sc.broadcast(senses)

    # Disambiguate contexts. Resulting graph is graph of word senses. 
    sense_graph_edges = contexts.map(lambda pair: disambiguate_context(senses.value, pair[0], pair[1][0], pair[1][1])).cache()

    # And cluster them
    if use_parallel_cw:
        edges_df = sense_graph_edges.flatMap(lambda pair: (Row(u=pair[0], v=v, weight=weight) for v, weight in pair[1])).toDF().cache()
        classes_df = sense_graph_edges.keys().map(lambda v: Row(v=v, cls=v)).toDF().cache()
        clusters_df = parallel_chinise_whispers(edges_df, classes_df, **global_cw_params)
        clusters_rdd = clusters_df.rdd.map(lambda x: x['cluster'])
    else:
        sense_graph = sense_graph_edges.collectAsMap()
        clusters = chinise_whispers(sense_graph, **global_cw_params)
        clusters.sort(key=len, reverse=True)
        clusters_rdd = sc.parallelize(clusters)

    save_synsets_to_file(id_to_word, clusters_rdd, output_path)
    
#######################################################
#######################################################
###   Main program
###    * connection to spark
###    * argument parsing
#######################################################
#######################################################

from pyspark.sql import SparkSession
import argparse

weighting_methods = ['TOP', 'DIST_LOG', 'DIST_NOLOG']

parser = argparse.ArgumentParser()
parser.add_argument('input_file', help='path to input file')
parser.add_argument('output_dir', help='path to output directory')
parser.add_argument('--local_cw_maxsteps', type=int, default=10)
parser.add_argument('--local_cw_weighting', choices=weighting_methods, default='TOP')
parser.add_argument('--global_cw_maxsteps', type=int, default=10)
parser.add_argument('--global_cw_weighting', choices=weighting_methods, default='TOP')
parser.add_argument('--no_parallel_cw', action='store_true', default=False,
                    help = "Use pure python Chinise Whispers instead of pyspark implementation, probably works faster")


if __name__ == "__main__":
    args = parser.parse_args()
    
    spark = SparkSession         .builder         .appName("Watset")         .getOrCreate()
    sc = spark.sparkContext
    
    watset(args.input_file, args.output_dir,
           local_cw_params={'max_steps' : args.local_cw_maxsteps, 'weights_mode' : args.local_cw_weighting},
           global_cw_params={'max_steps' : args.global_cw_maxsteps, 'weights_mode' : args.global_cw_weighting},
           use_parallel_cw=(not args.no_parallel_cw))
    
    spark.stop()
