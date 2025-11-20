def get_metadata(dataset_id, resource_id, fields, get_api_id = True):
    """
    Fetches specified metadata fields for both the dataset and a distribution (resource_id).
    If a field value is a resource URI, parses that resource but only returns its label/name (not the URI).
    Args:
        dataset_id (str): The dataset identifier (e.g. "510")
        resource_id (str): The distribution/resource identifier (e.g. "512")
        fields (list): List of metadata field names (e.g. ["modified", "description"])
    Returns:
        dict: {"dataset": {...}, "distribution": {...}}
    """
    from rdflib import Graph, Namespace, URIRef
    DCAT = Namespace("http://www.w3.org/ns/dcat#")
    DCT = Namespace("http://purl.org/dc/terms/")
    SCHEMA = Namespace("http://schema.org/")
    RDFS = Namespace("http://www.w3.org/2000/01/rdf-schema#")
    FOAF = Namespace("http://xmlns.com/foaf/0.1/")
    g = Graph()
    # Parse dataset RDF
    dataset_url = f"https://data.zg.ch/store/1/metadata/{dataset_id}"
    g.parse(dataset_url)
    # Parse all distribution RDFs
    distribution_uris = [
        str(o)
        for s, p, o in g.triples((None, DCAT.distribution, None))
    ]
    for uri in distribution_uris:
        g.parse(uri.replace("/resource/", "/metadata/"))
    # Prepare URIs
    dataset_ref = URIRef(f"https://data.zg.ch/store/1/resource/{dataset_id}")
    dist_ref = URIRef(f"https://data.zg.ch/store/1/resource/{resource_id}")
    # Map field names to predicates
    field_predicates = {
        "modified": DCT.modified,
        "description": DCT.description,
        "title": DCT.title,
        "issued": DCT.issued,
        "publisher": DCT.publisher,
        "format": DCT["format"],
        "downloadURL": DCAT.downloadURL,
        "accessURL": DCAT.accessURL,
        # Add more as needed
    }
    def get_label(uri):
        ref = URIRef(uri)
        for lbl in g.objects(ref, RDFS.label):
            return str(lbl)
        for lbl in g.objects(ref, FOAF.name):
            return str(lbl)
        return None
    def extract(ref):
        result = {}
        for field in fields:
            pred = field_predicates.get(field)
            if pred:
                values = [o for o in g.objects(ref, pred)]
                if values:
                    if isinstance(values[0], URIRef):
                        uri = str(values[0])
                        try:
                            g.parse(uri)
                        except Exception:
                            pass
                        label = get_label(uri)
                        if label:
                            result[field] = label
                    else:
                        val = str(values[0])
                        if val:
                            result[field] = val
        return result
    # --- API detection ---
    if get_api_id:
        for s, p, o in g.triples((None, DCT.source, dist_ref)):
            # s is the distribution that has dcterms:source == dist_ref
            s_str = str(s)
            if "/resource/" in s_str:
                api_id = s_str.split("/resource/")[-1]
                print(f"API found: {api_id}")
                
    return {
        "dataset": extract(dataset_ref),
        "distribution": extract(dist_ref)
    }
