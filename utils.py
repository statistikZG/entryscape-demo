import warnings

NAMESPACE = {
        "dcat": "http://www.w3.org/ns/dcat#",
        "dct": "http://purl.org/dc/terms/",
        "dcterms": "http://purl.org/dc/terms/",
        "schema": "http://schema.org/",
        "foaf": "http://xmlns.com/foaf/0.1/",
        "rdfs": "http://www.w3.org/2000/01/rdf-schema#"
    }


def get_resource_metadata_field(catalog_url, resource_id, field_uri, graph=None, all_values=False):
    """
    Fetches one or more values of a specific metadata field for a resource from an RDF catalog.

    Args:
        catalog_url (str): The base URL of the catalog.
        resource_id (str): The identifier of the resource.
        field_uri (str): The metadata field to fetch.
        graph (rdflib.Graph, optional): Pre-parsed RDF graph to use.
        all_values (bool): If True, return all values as a list.

    Returns:
        str, list, or None: The value(s) of the field, or None if not found.
    """
    from rdflib import Graph, URIRef, Namespace, Literal

    namespace = NAMESPACE

    if ":" in field_uri and not field_uri.startswith("http"):
        prefix, local = field_uri.split(":", 1)
        ns_uri = namespace.get(prefix)
        if ns_uri:
            pred = Namespace(ns_uri)[local]
        else:
            raise ValueError(f"Unknown namespace prefix: {prefix}")
    else:
        pred = URIRef(field_uri)

    g = graph or Graph()
    if not graph:
        resource_url = f"{catalog_url}/metadata/{resource_id}"
        try:
            g.parse(resource_url)
        except Exception as e:
            warnings.warn(f"Failed to parse RDF: {e}")
            return None

    resource_ref = URIRef(f"{catalog_url}/resource/{resource_id}")
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
    if len(distribution_uris) > 1:
        warnings.warn(f"More than one distribution found: {distribution_uris}")
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
                if accessURL:
                    api_endpoints.append(accessURL)
                else:   
                    continue

    if len(api_endpoints) > 1:
        warnings.warn(f"More than one API resource found: {api_endpoints}")
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