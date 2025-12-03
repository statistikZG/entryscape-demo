import warnings

NAMESPACE = {
        "dcat": "http://www.w3.org/ns/dcat#",
        "dct": "http://purl.org/dc/terms/",
        "dcterms": "http://purl.org/dc/terms/",
        "schema": "http://schema.org/",
        "foaf": "http://xmlns.com/foaf/0.1/",
        "rdfs": "http://www.w3.org/2000/01/rdf-schema#"
    }


def get_metadata_field(q_name, catalog_url=None, resource_id=None, resource_uri=None, graph=None, all_values=False):
    """
    Fetches one or more values of a specific metadata field for a resource from an RDF catalog.
    Allows input via resource_id (with catalog_url) or full resource_uri.

    Args:
        q_name (str): The qualified name or full URI of the metadata field.
        catalog_url (str, optional): The base URL of the catalog.
        resource_id (str, optional): The identifier of the resource.
        resource_uri (str, optional): The full URI of the resource.
        graph (rdflib.Graph, optional): Pre-parsed RDF graph to use.
        all_values (bool): If True, return all values as a list.

    Returns:
        str, list, or None: The value(s) of the field, or None if not found.
    """
    from rdflib import Graph, URIRef, Namespace, Literal
    import warnings

    namespace = NAMESPACE

    # Resolve predicate
    if not q_name:
        raise ValueError("field_uri must be provided")
    if ":" in q_name and not q_name.startswith("http"):
        prefix, local = q_name.split(":", 1)
        ns_uri = namespace.get(prefix)
        if ns_uri:
            pred = Namespace(ns_uri)[local]
        else:
            raise ValueError(f"Unknown namespace prefix: {prefix}")
    else:
        pred = URIRef(q_name)

    # Resolve resource reference and metadata URL
    if resource_id and catalog_url:
        resource_ref = URIRef(f"{catalog_url}/resource/{resource_id}")
        metadata_url = f"{catalog_url}/metadata/{resource_id}"
    elif resource_uri:
        resource_ref = URIRef(resource_uri)
        metadata_url = resource_uri.replace("/resource/", "/metadata/")
    else:
        raise ValueError("Either resource_id with catalog_url or resource_uri must be provided.")

    # Use provided graph or parse
    g = graph or Graph()
    if not graph:
        try:
            g.parse(metadata_url)
        except Exception as e:
            warnings.warn(f"Failed to parse RDF: {e}")
            return None

    values = [o.toPython() if isinstance(o, Literal) else str(o) for o in g.objects(resource_ref, pred)]
    if not values:
        return None
    return values if all_values else values[0]

def find_distributions(catalog_url, dataset_id, format_mime=None):
    """
    Identifies distributions within a dataset that are in JSON format.
    Args:
        catalog_url (str): The base URL of the catalog.
        dataset_id (str): The identifier of the dataset.
        format_mime (str): The MIME type to filter distributions by (E.g., "application/json").
    Returns:
        list: A list of resource IDs that are in JSON format.
    """
    from rdflib import Graph, Namespace, URIRef

    DCAT = Namespace(NAMESPACE["dcat"])
    DCTERMS = Namespace(NAMESPACE["dcterms"])

    g = Graph()
    dataset_url = f"{catalog_url}/metadata/{dataset_id}"
    try:
        g.parse(dataset_url)
    except Exception as e:
        warnings.warn(f"Failed to parse RDF: {e}")
        return []

    dataset_ref = URIRef(f"{catalog_url}/resource/{dataset_id}")
    distribution_uris = []
    for dist in g.objects(dataset_ref, DCAT.distribution):
        dist_graph = Graph()
        dist_graph.parse(dist)
        dist_str = str(dist)
        if format_mime:
            for fmt in dist_graph.objects(dist, DCTERMS["format"]):
                if format_mime in str(fmt).lower():
                    
                    distribution_uris.append(dist_str)
        else:
            distribution_uris.append(dist_str)
    return distribution_uris

def extract_metadata_value(graph, resource_ref, predicate):
    """
    Extracts the value of a specific metadata field from a given RDF graph and resource reference.
    Args:
        graph (rdflib.Graph): The RDF graph.
        resource_ref (rdflib.URIRef): The reference to the resource.
        predicate (rdflib.URIRef): The predicate for the metadata field.
    Returns:
        str or None: The value of the metadata field, or None if not found.
    from rdflib import Literal
    """
    from rdflib import Literal

    values = [o.toPython() if isinstance(o, Literal) else str(o) for o in graph.objects(resource_ref, predicate)]
    return values[0] if values else None

def find_api_endpoints(catalog_url, dataset_id):
    """
    Identifies distributions within a dataset that are API endpoints.
    Args:
        catalog_url (str): The base URL of the catalog.
        dataset_id (str): The identifier of the dataset.
    Returns:
        list: A list of resource IDs that are API endpoints.
    """
    from rdflib import Graph, Namespace, URIRef

    DCAT = Namespace(NAMESPACE["dcat"])
    DCTERMS = Namespace(NAMESPACE["dcterms"])

    g = Graph()
    dataset_url = f"{catalog_url}/metadata/{dataset_id}"
    try:
        g.parse(dataset_url)
    except Exception as e:
        warnings.warn(f"Failed to parse RDF: {e}")
        return []

    dataset_ref = URIRef(f"{catalog_url}/resource/{dataset_id}")
    api_endpoints = []
    for dist in g.objects(dataset_ref, DCAT.distribution):
        dist_graph = Graph()
        dist_graph.parse(dist)
        for typ in dist_graph.objects(dist, DCTERMS["conformsTo"]):
            if "swagger" in str(typ).lower():
                # extract the accessURL value
                accessURL = extract_metadata_value(dist_graph, dist, DCAT["accessURL"])
                title = extract_metadata_value(dist_graph, dist, DCTERMS["title"])
                if accessURL:
                    api_endpoints.append(
                        {
                            "title": title,
                            "accessURL": accessURL
                        }
                        )
                else:   
                    continue
    return api_endpoints


def get_rowstore_data(api_endpoint, query_params=None, fetch_all=False, limit=100, offset=0):
    """
    Fetches data from a RowStore API endpoint, with optional pagination handling.
    Args:
        api_endpoint (str): The URL of the RowStore API endpoint.
        query_params (dict, optional): Query parameters to include in the request.
        fetch_all (bool): If True, fetches all results by handling pagination.
        limit (int): Number of results per page/request.
        offset (int): Starting offset for results.
    Returns:
        dict or list: The JSON response from the API, or a list of all results if fetch_all is True.
    """
    import requests

    params = query_params.copy() if query_params else {}
    params['_limit'] = limit
    params['_offset'] = offset
    all_results = []

    while True:
        try:
            response = requests.get(api_endpoint, params=params)
            response.raise_for_status()
            data = response.json()
            results = data.get('results', [])
        except requests.RequestException as e:
            warnings.warn(f"Failed to fetch data from RowStore API: {e}")
            return None
        
        if fetch_all:
            all_results.extend(results)
            result_count = data.get('resultCount', len(results))
            if params['_offset'] + limit >= result_count or not results:
                break
            params['_offset'] += limit
        else:
            return results

    return all_results

def get_license_label(licennse_uri, language="de", vocbulary_version="20240716"):
    """
    Fetches the preferred label of a license from the DCAT-AP CH license vocabulary in the specified language.
    Args:
        licennse_uri (str): The URI of the license.
        language (str): The language code for the preferred label (default is "de").
        vocbulary_version (str): The version of the vocabulary to use (default is "20240716").
    Returns:
        str or None: The preferred label in the specified language, or None if not found.
    """
    from rdflib import Graph, Namespace, URIRef
    g = Graph()
    g.parse(f"https://www.dcat-ap.ch/vocabulary/licenses/{vocbulary_version}.rdf")

    SKOS = Namespace("http://www.w3.org/2004/02/skos/core#")
    license_uri = URIRef(licennse_uri)

    for label in g.objects(license_uri, SKOS.prefLabel):
        if label.language == language:
            # return the label string
            return label.toPython()
    return None