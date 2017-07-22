package org.myorg;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
 
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
  
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

 
public class MyMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
 
    private static final Log LOG = LogFactory.getLog(MyMapper.class);
 
    // Fprivate Text videoName = new Text();
 
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
 
        try {
 
            InputStream is = new ByteArrayInputStream(value.toString().getBytes());
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(is);
 
            doc.getDocumentElement().normalize();
 
            NodeList nList = doc.getElementsByTagName("row");
 
            for (int temp = 0; temp < nList.getLength(); temp++) {
 
                Node nNode = nList.item(temp);
 
                if (nNode.getNodeType() == Node.ELEMENT_NODE) {
 
                    Element eElement = (Element) nNode;
                    
                    // Reads the district name, performance and objective value to a variable
                    String distName = eElement.getElementsByTagName("District_Name").item(0).getTextContent();
                    String pjtObjective = eElement.getElementsByTagName("Project_Objectives_IHHL_BPL").item(0).getTextContent();
                    String pjtPerformance = eElement.getElementsByTagName("Project_Performance-IHHL_BPL").item(0).getTextContent();
                    
                    //filters out the districts which have obtained 100% objective
                    
                    if ((Integer.parseInt(pjtObjective)%Integer.parseInt(pjtPerformance))==0)
                    {
                    	 context.write(new Text(distName + "," + pjtObjective + "," + pjtPerformance), NullWritable.get());
                    	 System.out.println(distName + "," + pjtObjective + "," + pjtPerformance);
                    	
                    }
 
                    // System.out.println(id + "," + name + "," + gender);
                   
 
                }
            }
        } catch (Exception e) {
          
        }
 
    }
 
}