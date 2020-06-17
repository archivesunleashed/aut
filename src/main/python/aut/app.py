import base64
import hashlib
import os
from xml.sax.saxutils import escape

from pyspark.sql import DataFrame
from pyspark.sql.functions import desc, first


def ExtractPopularImages(d, limit, min_width, min_height):
    df = d.select("url", "md5")
    count = df.groupBy("md5").count()

    return (
        df.join(count, "md5")
        .groupBy("md5")
        .agg(first("url").alias("url"), first("count").alias("count"))
        .select("url", "count")
        .orderBy(desc("count"))
        .limit(limit)
    )


def SaveBytes(data, bytes_path):
    os.makedirs(bytes_path, exist_ok=True)

    for row in data:
        with open(
            bytes_path
            + "/"
            + hashlib.md5(base64.b64decode(row[1])).hexdigest()
            + "."
            + row[0],
            "wb",
        ) as f:
            f.write(base64.b64decode(row[1]))


def WriteGEXF(data, gexf_path):
    output_file = open(gexf_path, "x")
    end_attribute = '" />\n'
    vertices = set()

    for row in data:
        vertices.add(str(row[1]))
        vertices.add(str(row[2]))

    output_file.write(
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        + '<gexf xmlns="http://www.gexf.net/1.3draft"\n'
        + '  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"\n'
        + '  xsi:schemaLocation="http://www.gexf.net/1.3draft\n'
        + '                       http://www.gexf.net/1.3draft/gexf.xsd"\n'
        + '  version="1.3">\n'
        + '<graph mode="static" defaultedgetype="directed">\n'
        + '<attributes class="edge">\n'
        + '  <attribute id="0" title="crawlDate" type="string" />\n'
        + "</attributes>\n"
        + "<nodes>\n"
    )

    for vertice in vertices:
        output_file.write(
            '<node id="'
            + hashlib.md5(vertice.encode()).hexdigest()
            + '" label="'
            + escape(vertice)
            + end_attribute
        )

    output_file.write("</nodes>\n<edges>\n")

    for edge in data:
        output_file.write(
            '<edge source="'
            + hashlib.md5(edge[1].encode()).hexdigest()
            + '" target="'
            + hashlib.md5(edge[2].encode()).hexdigest()
            + '" weight="'
            + str(edge[3])
            + '" type="directed">\n'
            + "<attvalues>\n"
            + '<attvalue for="0" value="'
            + str(edge[0])
            + end_attribute
            + "</attvalues>\n"
            + "</edge>\n"
        )

    output_file.write("</edges>\n</graph>\n</gexf>")
    output_file.close()
    return


def WriteGraphML(data, graphml_path):
    output_file = open(graphml_path, "x")
    nodes = set()

    for row in data:
        nodes.add(str(row[1]))
        nodes.add(str(row[2]))

    output_file.write(
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        + '<graphml xmlns="http://graphml.graphdrawing.org/xmlns"\n'
        + '  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"\n'
        + '  xsi:schemaLocation="http://graphml.graphdrawing.org/xmlns\n'
        + '  http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd"\n>'
        + '<key id="label" for="node" attr.name="label" attr.type="string" />\n'
        + '<key id="weight" for="edge" attr.name="weight" attr.type="double">\n'
        + "<default>0.0</default>\n"
        + "</key>\n"
        + '<key id="crawlDate" for="edge" attr.name="crawlDate" attr.type="string" />\n'
        + '<graph mode="static" edgedefault="directed">\n'
    )

    for node in nodes:
        output_file.write(
            '<node id="'
            + hashlib.md5(node.encode()).hexdigest()
            + '">\n'
            + '<data key="label">'
            + escape(node)
            + "</data>\n</node>\n"
        )

    for edge in data:
        output_file.write(
            '<edge source="'
            + hashlib.md5(edge[1].encode()).hexdigest()
            + '" target="'
            + hashlib.md5(edge[2].encode()).hexdigest()
            + '" type="directed">\n'
            + '<data key="weight">'
            + str(row[3])
            + "</data>\n"
            + '<data key="crawlDate">'
            + str(row[0])
            + "</data>\n"
            + "</edge>\n"
        )

    output_file.write("</graph>\n</graphml>")
    output_file.close()
    return
