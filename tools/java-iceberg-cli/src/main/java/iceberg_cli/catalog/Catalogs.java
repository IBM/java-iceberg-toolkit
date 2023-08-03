/**
  * (c) Copyright IBM Corp. 2022. All Rights Reserved.
  */

package iceberg_cli.catalog;

import java.util.ArrayList;
import java.util.List;

public class Catalogs {
    List<CustomCatalog> catalogs;

    public Catalogs() {
        catalogs = new ArrayList<CustomCatalog>();
    }

    public void addCatalog(CustomCatalog catalog) {
        catalogs.add(catalog);
    }

    public List<CustomCatalog> getCatalogs() {
        return catalogs;
    }

    public CustomCatalog getCatalog(String catalogName, String metastoreUri) {
        if (catalogName == null || metastoreUri == null)
            return null;
        
        for (CustomCatalog catalog : catalogs) {
            if (catalog.getName().equalsIgnoreCase(catalogName) && catalog.getMetastoreUri().equalsIgnoreCase(metastoreUri)) {
                return catalog;
            }
        }
        return null;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (CustomCatalog catalog : catalogs) {
            sb.append(catalog);
            sb.append("\n");
        }

        return sb.toString();
    }
}
