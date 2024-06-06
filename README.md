# geopipeline

Fetches, transforms, and combines public data into GeoJSON or Shapefiles for use in making maps.

This is still very much a work in progress. In particular, the pipeline schema configuration is likely
to change in the near future.

The following sample workflows are currently configured:
 - **transit**: Outputs a map illustrating public transit bus frequencies in the Chicago area.
 - **osmdev**: Filters OpenStreetMap roads data for the state of Illinois down to just the Chicago area.
 - **bikemap**: Combines bicycle facility information and street information for the City of Chicago and runs routing simulations to identify streets that are good through routes for bicycling.
