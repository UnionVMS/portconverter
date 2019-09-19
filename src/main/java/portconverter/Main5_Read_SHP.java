package portconverter;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Map;

import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureSource;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.opengis.feature.Feature;
import org.opengis.feature.GeometryAttribute;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;

public class Main5_Read_SHP {

	private void go(String[] args) throws IOException {
		
		
		File file = new File("C:\\_ref_countries\\ref-countries-2016-01m.shp\\CNTR_RG_01M_2016_4326.shp\\CNTR_RG_01M_2016_4326.shp");

		Map<String, Object> map = new HashMap<>();
        map.put("url", file.toURI().toURL());

        DataStore dataStore = DataStoreFinder.getDataStore(map);
        String typeName = dataStore.getTypeNames()[0];

        FeatureSource<SimpleFeatureType, SimpleFeature> source =
                dataStore.getFeatureSource(typeName);
        Filter filter = Filter.INCLUDE; // ECQL.toFilter("BBOX(THE_GEOM, 10,20,30,40)")

        FeatureCollection<SimpleFeatureType, SimpleFeature> collection = source.getFeatures(filter);
        try (FeatureIterator<SimpleFeature> features = collection.features()) {
            if (features.hasNext()) {
                SimpleFeature feature = features.next();
                int n = feature.getAttributeCount();
                for(int i = 0 ; i < n ; i++) {
                	Object o = feature.getAttribute(i);
                	System.out.println(o.toString());
                }
            }
        }
	}

	public static void main(String[] args) throws IOException {
		Main5_Read_SHP obj = new Main5_Read_SHP();
		obj.go(args);
	}

}
