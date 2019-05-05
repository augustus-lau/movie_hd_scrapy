package cn.moviebigdata.mdata.conf;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.xml.sax.SAXException;

public class MovieRepository {
	
	
	private static final Log LOG = LogFactory.getLog(MovieRepository.class);
	
	private static final String URL_FETCH_FILE="fetch.url.file";
	
	private static final Map<String,MovieRepository> CACHE = new HashMap<String,MovieRepository>();
	
	private List<MovieDescription> descripts;
	
	private String fetch_file;
	
	private MovieRepository(Configuration conf) {
		descripts = new ArrayList<MovieDescription>();
		fetch_file = conf.get(URL_FETCH_FILE);
	}

	public synchronized static MovieRepository buildRepository(Configuration conf) {
		MovieRepository repository;
		String CACHE_KEY = "movie@hadoop"+conf.hashCode();
		repository = CACHE.get(CACHE_KEY);
		if(repository!=null){
			LOG.info("reading repository info from cache...");
			return repository;
		}else{
			LOG.info("reading repository info from file...");
			repository = new MovieRepository(conf);
			InputStream input = null;
			if(StringUtils.isEmpty(repository.getFetch_file())){
				input =Thread.currentThread().getContextClassLoader().getResourceAsStream("mdata-urls.xml");
			}else{
				input =Thread.currentThread().getContextClassLoader().getResourceAsStream(repository.getFetch_file());
			}
			UrlRepositoryFeeder feeder = new UrlRepositoryFeeder(repository);
			SAXParserFactory spf = SAXParserFactory.newInstance();
			try {
				SAXParser saxParser = spf.newSAXParser();
				saxParser.parse(input, feeder);
				CACHE.put(CACHE_KEY, repository);
				return repository;
			} catch (ParserConfigurationException e) {
				e.printStackTrace();
			} catch (SAXException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			return null;
		}
	}

	public void addDescription(MovieDescription desc) {
		descripts.add(desc);
	}
	
	/**
	 * @return the fetch_file
	 */
	public String getFetch_file() {
		return fetch_file;
	}

	/**
	 * @param fetch_file the fetch_file to set
	 */
	public void setFetch_file(String fetch_file) {
		this.fetch_file = fetch_file;
	}

	public List<MovieDescription> getMovieDescriptions(){
		return descripts;
	}
	
	public int getDescSize(){
		if(descripts==null){
			return 0;
		}else{
			return descripts.size();
		}
	}
}
