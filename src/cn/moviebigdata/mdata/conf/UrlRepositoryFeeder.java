package cn.moviebigdata.mdata.conf;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class UrlRepositoryFeeder extends DefaultHandler{
	
	private String tagName ;
	
	private boolean parasestart = false;
	
	private MovieRepository repository;
	
	private MovieDescription description;
	
	public UrlRepositoryFeeder(MovieRepository repository) {
		this.repository = repository;
	}

	/* (non-Javadoc)
	 * @see org.xml.sax.helpers.DefaultHandler#startElement(java.lang.String, java.lang.String, java.lang.String, org.xml.sax.Attributes)
	 */
	@Override
	public void startElement(String uri, String localName, String qName,
			Attributes attributes) throws SAXException {
		if(parasestart){
			if(qName.equals("param-format")){
				description.setUrlpattern(attributes.getValue("pattern"));
			}
			tagName = qName;
		}else if(!parasestart && qName.equals("url")){
			parasestart = true;
			qName = null;
			description = new MovieDescription();
		}
	}
	
	
	
	/* (non-Javadoc)
	 * @see org.xml.sax.helpers.DefaultHandler#endElement(java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public void endElement(String uri, String localName, String qName)
			throws SAXException {
		if(parasestart && qName.equals("url")){
			repository.addDescription(description);
			parasestart = false;
			description = null;
		}
	}

	/* (non-Javadoc)
	 * @see org.xml.sax.helpers.DefaultHandler#characters(char[], int, int)
	 */
	@Override
	public void characters(char[] ch, int start, int length)
			throws SAXException {
		if(parasestart && tagName!=null){
			if(tagName.equals("name")){
				description.setName(new String(ch,start,length));
			}else if(tagName.equals("point")){
				description.setPoint(new String(ch,start,length));
			}else if(tagName.equals("data-type")){
				description.setType(new String(ch,start,length));
			}
			tagName = null;
		}
	}

}
