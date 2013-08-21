package com.aamend.hadoop.mapreduce.join;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.aamend.hadoop.mapreduce.job.MRDPUtils;

public class PostCommentHierarchy {

	private DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		// First call, map Posts with Comments (whatever the question, answer)
		Job job = new Job(conf, "PostCommentHierarchy");
		job.setJarByClass(PostCommentHierarchy.class);

		MultipleInputs.addInputPath(job, new Path(args[0]),
				TextInputFormat.class, PostMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]),
				TextInputFormat.class, CommentMapper.class);

		job.setReducerClass(PostCommentJoinReducer.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		TextOutputFormat.setOutputPath(job, new Path(args[2]));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.waitForCompletion(true);

		// Second call, map Question Posts with Answers Posts
		job = new Job(conf, "QuestionAnswerHierarchy");
		job.setJarByClass(PostCommentHierarchy.class);

		TextInputFormat.setInputPaths(job, new Path(args[2]));
		TextOutputFormat.setOutputPath(job, new Path(args[3]));
		
		job.setMapperClass(QuestionAnswerMapper.class);
		job.setReducerClass(QuestionAnswerReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		System.exit(job.waitForCompletion(true) ? 0 : 2);
	}

	public class PostMapper extends Mapper<Object, Text, Text, Text> {

		private Text outkey = new Text();
		private Text outvalue = new Text();

		public void map(Object text, Text value, Context context)
				throws IOException, InterruptedException {

			Map<String, String> parsed = MRDPUtils.xmlToMap(value.toString());
			// The foreign join key is the post ID
			outkey.set(parsed.get("Id"));
			// Flag this record as P for the reducer and then output
			outvalue.set("P" + value.toString());
			context.write(outkey, outvalue);
		}

	}

	public class CommentMapper extends Mapper<Object, Text, Text, Text> {

		private Text outkey = new Text();
		private Text outvalue = new Text();

		public void map(Object text, Text value, Context context)
				throws IOException, InterruptedException {

			Map<String, String> parsed = MRDPUtils.xmlToMap(value.toString());
			// The foreign join key is the post ID
			outkey.set(parsed.get("PostId"));
			// Flag this record for the reducer and then output
			outvalue.set("C" + value.toString());
			context.write(outkey, outvalue);
		}
	}

	public class PostCommentJoinReducer extends
			Reducer<Text, Text, Text, NullWritable> {

		private ArrayList<String> comments = new ArrayList<String>();
		private String post = null;

		public void reduce(Text text, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			// Reset variables
			post = null;
			comments.clear();
			// For each input value
			for (Text t : values) {
				// If this is the post record, store it, minus the flag
				if (t.charAt(0) == 'P') {
					post = t.toString().substring(1, t.toString().length())
							.trim();
				} else {
					// Else, it is a comment record. Add it to the list, minus
					// the flag
					comments.add(t.toString()
							.substring(1, t.toString().length()).trim());
				}
			}
			// If there are no comments, the comments list will simply be empty.
			// If post is not null, combine post with its comments.
			if (post != null) {
				// nest the comments underneath the post element
				String postWithCommentChildren;
				try {
					postWithCommentChildren = nestElements(post, comments);
				} catch (TransformerException e) {
					return;
				} catch (SAXException e) {
					return;
				} catch (ParserConfigurationException e) {
					return;
				}

				context.write(new Text(postWithCommentChildren),
						NullWritable.get());

			}
		}
	}

	public class QuestionAnswerMapper extends Mapper<Object, Text, Text, Text> {
		
		private Text outkey = new Text();
		private Text outvalue = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// Parse the post/comment XML hierarchy into an Element
			Element post;
			try {
				post = getXmlElementFromString(value.toString());
			} catch (SAXException e) {
				return;
			} catch (ParserConfigurationException e) {
				return;
			}
			int postType = Integer.parseInt(post.getAttribute("PostTypeId"));
			// If postType is 1, it is a question
			if (postType == 1) {
				outkey.set(post.getAttribute("Id"));
				outvalue.set("Q" + value.toString());
			} else {
				// Else, it is an answer
				outkey.set(post.getAttribute("ParentId"));
				outvalue.set("A" + value.toString());
			}
			context.write(outkey, outvalue);
		}

		private Element getXmlElementFromString(String xml)
				throws SAXException, IOException, ParserConfigurationException {
			// Create a new document builder
			DocumentBuilder bldr = dbf.newDocumentBuilder();
			return bldr.parse(new InputSource(new StringReader(xml)))
					.getDocumentElement();
		}
	}

	public class QuestionAnswerReducer extends
			Reducer<Text, Text, Text, NullWritable> {
		private ArrayList<String> answers = new ArrayList<String>();
		private String question = null;

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// Reset variables
			question = null;
			answers.clear();
			// For each input value
			for (Text t : values) {
				// If this is the post record, store it, minus the flag
				if (t.charAt(0) == 'Q') {
					question = t.toString().substring(1, t.toString().length())
							.trim();
				} else {
					// Else, it is a comment record. Add it to the list,
					// minus
					// the flag
					answers.add(t.toString()
							.substring(1, t.toString().length()).trim());
				}
			}
			// If post is not null
			if (question != null) {
				// nest the comments underneath the post element
				String postWithCommentChildren;
				try {
					postWithCommentChildren = nestElements(question, answers);
				} catch (TransformerException e) {
					return;
				} catch (SAXException e) {
					return;
				} catch (ParserConfigurationException e) {
					return;
				}
				// write out the XML
				context.write(new Text(postWithCommentChildren),
						NullWritable.get());
			}
		}
	}

	private String nestElements(String post, List<String> comments)
			throws TransformerException, SAXException, IOException,
			ParserConfigurationException {
		// Create the new document to build the XML
		DocumentBuilder bldr = dbf.newDocumentBuilder();
		Document doc = bldr.newDocument();
		// Copy parent node to document
		Element postEl = getXmlElementFromString(post);
		Element toAddPostEl = doc.createElement("post");
		// Copy the attributes of the original post element to the new one
		copyAttributesToElement(postEl.getAttributes(), toAddPostEl);
		// For each comment, copy it to the "post" node
		for (String commentXml : comments) {
			Element commentEl = getXmlElementFromString(commentXml);
			Element toAddCommentEl = doc.createElement("comments");
			// Copy the attributes of the original comment element to
			// the new one
			copyAttributesToElement(commentEl.getAttributes(), toAddCommentEl);
			// Add the copied comment to the post element
			toAddPostEl.appendChild(toAddCommentEl);
		}
		// Add the post element to the document
		doc.appendChild(toAddPostEl);
		// Transform the document into a String of XML and return
		return transformDocumentToString(doc);
	}

	private Element getXmlElementFromString(String xml) throws SAXException,
			IOException, ParserConfigurationException {
		// Create a new document builder
		DocumentBuilder bldr = dbf.newDocumentBuilder();
		return bldr.parse(new InputSource(new StringReader(xml)))
				.getDocumentElement();
	}

	private void copyAttributesToElement(NamedNodeMap attributes,
			Element element) {
		// For each attribute, copy it to the element
		for (int i = 0; i < attributes.getLength(); ++i) {
			Attr toCopy = (Attr) attributes.item(i);
			element.setAttribute(toCopy.getName(), toCopy.getValue());
		}
	}

	private String transformDocumentToString(Document doc)
			throws TransformerException {
		TransformerFactory tf = TransformerFactory.newInstance();
		Transformer transformer = tf.newTransformer();
		transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
		StringWriter writer = new StringWriter();
		transformer.transform(new DOMSource(doc), new StreamResult(writer));
		// Replace all new line characters with an empty string to have
		// one record per line.
		return writer.getBuffer().toString().replaceAll("\n|\r", "");
	}
}