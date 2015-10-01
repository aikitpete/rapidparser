package com.piglin.testing;

import com.mongodb.*;
import com.mongodb.util.JSON;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.*;
import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Hello world!
 */
public class AppBackup {
    public static void main(String[] args) {

        try {

            DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder documentBuilder;

            XPathFactory xPathfactory = XPathFactory.newInstance();
            XPath xpath = xPathfactory.newXPath();

            File processFile = new File("/Users/swyna/.RapidMiner/repositories/Local Repository/Mongo to CSV.rmp");

            documentBuilder = documentBuilderFactory.newDocumentBuilder();
            Document processDocument = documentBuilder.parse(processFile);
            processDocument.getDocumentElement().normalize();

            File databaseFile = new File("/Users/swyna/.RapidMiner/repositories/Local Repository/UNI Rankings Database.rmp");

            documentBuilder = documentBuilderFactory.newDocumentBuilder();
            Document databaseDocument = documentBuilder.parse(databaseFile);
            databaseDocument.getDocumentElement().normalize();

            XPathExpression expression;
            NodeList nodeList;

            //Node copiedNode;
            Element newElement;
            Element newElement2;
            Element newElement3;
            Node expandedNode;
            Node expandedNodeDatabase;

            expression = xpath.compile("//*[@id='Expanded']");

            nodeList = (NodeList) expression.evaluate(processDocument, XPathConstants.NODESET);

            expandedNode = nodeList.item(0);

            nodeList = (NodeList) expression.evaluate(databaseDocument, XPathConstants.NODESET);

            expandedNodeDatabase = nodeList.item(0);

            removeAll(expandedNode);
            removeAll(expandedNodeDatabase);

            /*
            File templateFile = new File("./RapidMiner_Template.xml");
            documentBuilder = documentBuilderFactory.newDocumentBuilder();
            Document templateDocument = documentBuilder.parse(templateFile);

            Element templateElement = templateDocument.getDocumentElement();
            Element temporaryElement;

            expression = xpath.compile("//parameter[@id='COLLECTION_NAME']");
            nodeList = (NodeList) expression.evaluate(templateDocument, XPathConstants.NODESET);
            Node collectionNameNode = nodeList.item(0);


            expression = xpath.compile("//parameter[@id='SCORE_OLD']");
            nodeList = (NodeList) expression.evaluate(templateDocument, XPathConstants.NODESET);
            Node scoreOldNode = nodeList.item(0);


            expression = xpath.compile("//parameter[@id='SCORE_NEW']");
            nodeList = (NodeList) expression.evaluate(templateDocument, XPathConstants.NODESET);
            Node scoreNewNode = nodeList.item(0);


            expression = xpath.compile("//list[@id='ADDITIONAL']/parameter/@key");
            nodeList = (NodeList) expression.evaluate(templateDocument, XPathConstants.NODESET);
            Node keyNode = nodeList.item(0);


            expression = xpath.compile("//list[@id='ADDITIONAL']/parameter/@value");
            nodeList = (NodeList) expression.evaluate(templateDocument, XPathConstants.NODESET);
            Node valueNode = nodeList.item(0);


            expression = xpath.compile("//parameter[@id='ATTRIBUTES']");
            nodeList = (NodeList) expression.evaluate(templateDocument, XPathConstants.NODESET);
            Node attributesNode = nodeList.item(0);

            expression = xpath.compile("//parameter[@id='FILENAME']");
            nodeList = (NodeList) expression.evaluate(templateDocument, XPathConstants.NODESET);
            Node filenameNode = nodeList.item(0);
            */

            String connect1Name;
            String connect2Name;
            String connect3Name;
            String connect4Name;
            String connect5Name;
            String databaseName;
            String baseName;
            String fullName;
            String attributes;
            String filename;

            File f = new File(".");

            ArrayList<String> names = new ArrayList<String>(Arrays.asList(f.list()));

            Mongo mongo = new Mongo("localhost", 27017);
            DB db = mongo.getDB("universityrankings");

            for (int i=0; i<names.size(); i++) {

                if (!names.get(i).matches(".*.json")) {
                    System.out.println("Skipping: "+names.get(i));
                    continue;
                }

                DBCollection collection = db.getCollection(names.get(i).substring(0,names.get(i).lastIndexOf('.')));

                System.out.println("Collection name: "+names.get(i).substring(0,names.get(i).lastIndexOf('.')));

                String collectionData = readFile(names.get(i), StandardCharsets.UTF_8);

                // convert JSON to DBObject directly
                //DBObject dbObject = (DBObject) JSON.parse(collectionData);
                DBObject dbObject= (DBObject) JSON.parse(collectionData);

                int index = 0;
                DBObject current = (DBObject) dbObject.get(""+index);

                collection.drop();


                while (current!=null) {
                    collection.insert(current);

                    index++;
                    current = (DBObject) dbObject.get(""+index);
                }

                DBCursor cursorDoc = collection.find();

                //while (cursorDoc.hasNext()) {
                    //System.out.println(cursorDoc.next());
                //}

                //newElement = templateElement;

                /*
                collectionNameNode.setNodeValue(names.get(i).substring(0,names.get(i).lastIndexOf('.')));
                scoreOldNode.setNodeValue("score");
                scoreNewNode.setNodeValue("score_" + names.get(i).substring(0, names.get(i).lastIndexOf('.')));

                keyNode.setNodeValue("rank");
                valueNode.setNodeValue("rank_" + names.get(i).substring(names.get(i).lastIndexOf("2014QS") + 1, names.get(i).lastIndexOf('.')).toLowerCase());

                attributesNode.setNodeValue("score_overall|score_computer|title|rank_overall|rank_computer|country_tid|region_tid|rank_times|score_times|score_arwu|country|rank_arwu|rank_communication|score_communication");

                filenameNode.setNodeValue(names.get(i).substring(0, names.get(i).lastIndexOf('.')) + ".csv");

                for (int j=0; j<templateElement.getChildNodes().getLength();j++) {

                    //System.out.println(templateElement.getChildNodes().getLength());
                    copiedNode = processDocument.importNode(templateElement.getChildNodes().item(j), true);
                    expandedNode.appendChild(copiedNode);

                }
                */

                databaseName = names.get(i).substring(0, names.get(i).lastIndexOf('.'));
                baseName = databaseName.replaceFirst("2014QS","");
                System.out.println(baseName);

                filename = "/Users/swyna/Sites/Webparser/"+names.get(i).substring(0, names.get(i).lastIndexOf('.')) + ".csv";
                attributes =  "title|country_tid|region_tid|rank_"+baseName.toLowerCase()+"|score_"+baseName.toLowerCase();

                //fullName = baseName + " " + i;

                connect1Name = baseName + " DB";
                connect2Name = baseName + " Data";
                connect3Name = baseName + " Rename";
                connect4Name = baseName + " Attributes";
                connect5Name = baseName + " CSV";

                newElement = processDocument.createElement("operator");
                newElement.setAttribute("activated","true");
                newElement.setAttribute("class","nosql:mongodb_document_reader");
                newElement.setAttribute("compatibility","6.1.001");
                newElement.setAttribute("expanded","true");
                newElement.setAttribute("name", connect1Name);

                newElement2 = processDocument.createElement("parameter");
                newElement2.setAttribute("key","mongodb_instance");
                newElement2.setAttribute("value","Local MongoDB");
                newElement.appendChild(newElement2);

                newElement2 = processDocument.createElement("parameter");
                newElement2.setAttribute("key","collection");
                newElement2.setAttribute("value",databaseName);
                newElement.appendChild(newElement2);

                newElement2 = processDocument.createElement("parameter");
                newElement2.setAttribute("key","sort_documents");
                newElement2.setAttribute("value","false");
                newElement.appendChild(newElement2);

                newElement2 = processDocument.createElement("parameter");
                newElement2.setAttribute("key","limit_results");
                newElement2.setAttribute("value","false");
                newElement.appendChild(newElement2);

                newElement2 = processDocument.createElement("parameter");
                newElement2.setAttribute("key","skip");
                newElement2.setAttribute("value","0");
                newElement.appendChild(newElement2);

                expandedNode.appendChild(newElement);

                newElement = processDocument.createElement("operator");
                newElement.setAttribute("activated","true");
                newElement.setAttribute("class","text:json_to_data");
                newElement.setAttribute("compatibility","6.1.001");
                newElement.setAttribute("expanded","true");
                newElement.setAttribute("name", connect2Name);

                newElement2 = processDocument.createElement("parameter");
                newElement2.setAttribute("key","ignore_attributes");
                newElement2.setAttribute("value","false");
                newElement.appendChild(newElement2);

                newElement2 = processDocument.createElement("parameter");
                newElement2.setAttribute("key","limit_attributes");
                newElement2.setAttribute("value","false");
                newElement.appendChild(newElement2);

                newElement2 = processDocument.createElement("parameter");
                newElement2.setAttribute("key","skip_invalid_documents");
                newElement2.setAttribute("value","false");
                newElement.appendChild(newElement2);

                expandedNode.appendChild(newElement);

                newElement = processDocument.createElement("operator");
                newElement.setAttribute("activated","true");
                newElement.setAttribute("class","rename");
                newElement.setAttribute("compatibility","6.1.001");
                newElement.setAttribute("expanded","true");
                newElement.setAttribute("name", connect3Name);

                newElement2 = processDocument.createElement("parameter");
                newElement2.setAttribute("key","old_name");
                newElement2.setAttribute("value","rank");
                newElement.appendChild(newElement2);

                newElement2 = processDocument.createElement("parameter");
                newElement2.setAttribute("key","new_name");
                newElement2.setAttribute("value","rank_"+baseName.toLowerCase());
                newElement.appendChild(newElement2);

                newElement2 = processDocument.createElement("list");
                newElement2.setAttribute("key","rename_additional_attributes");
                newElement3 = processDocument.createElement("parameter");
                newElement3.setAttribute("key","score_overall");
                newElement3.setAttribute("value","score_" + baseName.toLowerCase());
                newElement2.appendChild(newElement3);
                newElement.appendChild(newElement2);

                expandedNode.appendChild(newElement);

                newElement = processDocument.createElement("operator");
                newElement.setAttribute("activated","true");
                newElement.setAttribute("class","select_attributes");
                newElement.setAttribute("compatibility","6.1.001");
                newElement.setAttribute("expanded","true");
                newElement.setAttribute("name", connect4Name);

                newElement2 = processDocument.createElement("parameter");
                newElement2.setAttribute("key","attribute_filter_type");
                newElement2.setAttribute("value","subset");
                newElement.appendChild(newElement2);

                newElement2 = processDocument.createElement("parameter");
                newElement2.setAttribute("key","attribute");
                newElement2.setAttribute("value","");
                newElement.appendChild(newElement2);

                newElement2 = processDocument.createElement("parameter");
                newElement2.setAttribute("key","attributes");
                newElement2.setAttribute("value",attributes);
                newElement.appendChild(newElement2);

                newElement2 = processDocument.createElement("parameter");
                newElement2.setAttribute("key","use_except_expression");
                newElement2.setAttribute("value","false");
                newElement.appendChild(newElement2);

                newElement2 = processDocument.createElement("parameter");
                newElement2.setAttribute("key","value_type");
                newElement2.setAttribute("value","attribute_value");
                newElement.appendChild(newElement2);

                newElement2 = processDocument.createElement("parameter");
                newElement2.setAttribute("key","use_value_time_exception");
                newElement2.setAttribute("value","false");
                newElement.appendChild(newElement2);

                newElement2 = processDocument.createElement("parameter");
                newElement2.setAttribute("key","except_value_type");
                newElement2.setAttribute("value","time");
                newElement.appendChild(newElement2);

                newElement2 = processDocument.createElement("parameter");
                newElement2.setAttribute("key","block_type");
                newElement2.setAttribute("value","attribute_block");
                newElement.appendChild(newElement2);

                newElement2 = processDocument.createElement("parameter");
                newElement2.setAttribute("key","use_block_type_exception");
                newElement2.setAttribute("value","false");
                newElement.appendChild(newElement2);

                newElement2 = processDocument.createElement("parameter");
                newElement2.setAttribute("key","except_block_type");
                newElement2.setAttribute("value","value_matrix_row_start");
                newElement.appendChild(newElement2);

                newElement2 = processDocument.createElement("parameter");
                newElement2.setAttribute("key","invert_selection");
                newElement2.setAttribute("value","false");
                newElement.appendChild(newElement2);

                newElement2 = processDocument.createElement("parameter");
                newElement2.setAttribute("key","include_special_attributes");
                newElement2.setAttribute("value","false");
                newElement.appendChild(newElement2);

                newElement2 = processDocument.createElement("parameter");
                newElement2.setAttribute("key","new_name");
                newElement2.setAttribute("value","rank_"+baseName.toLowerCase());
                newElement.appendChild(newElement2);

                newElement2 = processDocument.createElement("parameter");
                newElement2.setAttribute("key","new_name");
                newElement2.setAttribute("value","rank_"+baseName.toLowerCase());
                newElement.appendChild(newElement2);

                expandedNode.appendChild(newElement);

                newElement = processDocument.createElement("operator");
                newElement.setAttribute("activated","true");
                newElement.setAttribute("class","write_csv");
                newElement.setAttribute("compatibility","6.1.001");
                newElement.setAttribute("expanded","true");
                newElement.setAttribute("name", connect5Name);

                newElement2 = processDocument.createElement("parameter");
                newElement2.setAttribute("key","csv_file");
                newElement2.setAttribute("value",filename);
                newElement.appendChild(newElement2);

                newElement2 = processDocument.createElement("parameter");
                newElement2.setAttribute("key","column_separator");
                newElement2.setAttribute("value",";");
                newElement.appendChild(newElement2);

                newElement2 = processDocument.createElement("parameter");
                newElement2.setAttribute("key","write_attribute_names");
                newElement2.setAttribute("value","true");
                newElement.appendChild(newElement2);

                newElement2 = processDocument.createElement("parameter");
                newElement2.setAttribute("key","quote_nominal_values");
                newElement2.setAttribute("value","true");
                newElement.appendChild(newElement2);

                newElement2 = processDocument.createElement("parameter");
                newElement2.setAttribute("key","format_date_attributes");
                newElement2.setAttribute("value","true");
                newElement.appendChild(newElement2);

                newElement2 = processDocument.createElement("parameter");
                newElement2.setAttribute("key","append_to_file");
                newElement2.setAttribute("value","false");
                newElement.appendChild(newElement2);

                newElement2 = processDocument.createElement("parameter");
                newElement2.setAttribute("key","encoding");
                newElement2.setAttribute("value","SYSTEM");
                newElement.appendChild(newElement2);

                expandedNode.appendChild(newElement);

                newElement = processDocument.createElement("connect");
                newElement.setAttribute("from_op",connect1Name);
                newElement.setAttribute("from_port","collection");
                newElement.setAttribute("to_op",connect2Name);
                newElement.setAttribute("to_port","documents 1");

                expandedNode.appendChild(newElement);

                newElement = processDocument.createElement("connect");
                newElement.setAttribute("from_op",connect2Name);
                newElement.setAttribute("from_port","example set");
                newElement.setAttribute("to_op",connect3Name);
                newElement.setAttribute("to_port","example set input");

                expandedNode.appendChild(newElement);

                newElement = processDocument.createElement("connect");
                newElement.setAttribute("from_op",connect3Name);
                newElement.setAttribute("from_port","example set output");
                newElement.setAttribute("to_op",connect4Name);
                newElement.setAttribute("to_port","example set input");

                expandedNode.appendChild(newElement);

                newElement = processDocument.createElement("connect");
                newElement.setAttribute("from_op",connect4Name);
                newElement.setAttribute("from_port","example set output");
                newElement.setAttribute("to_op",connect5Name);
                newElement.setAttribute("to_port","input");

                expandedNode.appendChild(newElement);

            }
            
            createJoins();
            
            newElement = processDocument.createElement("portSpacing");
            newElement.setAttribute("port","source_input 1");
            newElement.setAttribute("spacing","0");

            expandedNode.appendChild(newElement);
            expandedNodeDatabase.appendChild(newElement);

            newElement = processDocument.createElement("portSpacing");
            newElement.setAttribute("port","sink_result 1");
            newElement.setAttribute("spacing","0");

            expandedNode.appendChild(newElement);
            expandedNodeDatabase.appendChild(newElement);

            System.out.println("Done");

            // write the content into xml file
            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer transformer = transformerFactory.newTransformer();
            DOMSource source = new DOMSource(processDocument);
            StreamResult result = new StreamResult(new File("/Users/swyna/.RapidMiner/repositories/Local Repository/Mongo to CSV.rmp"));

            // Output to console for testing
            // StreamResult result = new StreamResult(System.out);

            transformer.transform(source, result);

            System.out.println("File saved!");

            mongo.close();

        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (MongoException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (TransformerConfigurationException e) {
            e.printStackTrace();
        } catch (TransformerException e) {
            e.printStackTrace();
        } catch (XPathExpressionException e) {
            e.printStackTrace();
        }

        /*try {
            System.out.println("Hello World!");

            URL url = null;

            url = new URL("http://www.topuniversities.com/university-rankings/world-university-rankings/2014#sorting=rank+region=+country=+faculty=+stars=false+search=");

            URLConnection uc = url.openConnection();

            InputStreamReader input = new InputStreamReader(uc.getInputStream());
            BufferedReader in = new BufferedReader(input);
            String inputLine;

            FileWriter outFile = new FileWriter("orhancan");
            PrintWriter out = new PrintWriter(outFile);

            while ((inputLine = in.readLine()) != null) {
                out.println(inputLine);
            }

            in.close();
            out.close();

            File fXmlFile = new File("orhancan");
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(fXmlFile);


            NodeList prelist = doc.getElementsByTagName("body");
            System.out.println(prelist.getLength());

        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }*/
    }

    private static void createJoins() {
    }

    public static void removeAll(Node node)
    {
        if (!node.hasChildNodes()) {
            return;
        }
        NodeList nodeList = node.getChildNodes();
        Node n;

        for(int i=0; i<nodeList.getLength();i++)
        {
            n = nodeList.item(i);

            if(n.hasChildNodes()) //edit to remove children of children
            {
                removeAll(n);
                node.removeChild(n);
            }
            else
                node.removeChild(n);
        }
    }

    static String readFile(String path, Charset encoding)
            throws IOException
    {
        byte[] encoded = Files.readAllBytes(Paths.get(path));
        return new String(encoded, encoding);
    }
}
