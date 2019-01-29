# Load required libraries
library(httr)
#' cds_retrieve
#'
#' Function to retrieve data from the Copernicus Climate Change Service (C3S) Climate Data Store
#'
#' @param dataset Dataset to extract data from.
#' @param request JSON string containing request parameters.
#' @param target  Filename to save data to.
#' @param request_id Previous request ID returned by function.
#' @return List containing request ID and name of data file downloaded, NA returned if timeout
#' @note API Key read from ~/.cdsapirc
#' @examples
#' dataset <- "reanalysis-era5-single-levels"
#' request <- "{
#'             'variable': 'mean_sea_level_pressure',
#'             'grid': ['0.1', '0.1'], 'product_type': 'reanalysis',
#'             'year': '2010', 'month': '08', 'day': '15', 'time': '12:00',
#'             'area': '45.0/-15.0/70.0/25.0',
#'             'format': 'netcdf'
#'             }"
#' result <- cds_retrieve( dataset, request)
#' # If timeout the following can be used to try again without generating new request
#' result <- cds_retrieve( dataset, request, request_id = result$request_id)
#'
#' @export
cds_retrieve <- function(dataset, request, target = '' , request_id = NA){
   # load connection details
   cdsConn <- readLines('~/.cdsapirc')
   # extract CDS URL
   cdsURL  <- cdsConn[ grep('url', cdsConn)   ]
   cdsURL  <- gsub( 'url: ', '', cdsURL)
   # extract CDS authentication details
   cdsKey  <- cdsConn[ grep('key', cdsConn) ]
   cdsUID  <- unlist( strsplit( cdsKey, ':') )[2]
   cdsUID  <- gsub( "\\s+","",cdsUID)
   cdsPWD  <- unlist( strsplit( cdsKey, ':') )[3]
   if (is.na( request_id) ){
      # Now we should be ready to connect and retrieve the data
      # first strip any extra whitespace from request string
      req <- gsub('\\s+',' ',request)
      # Now post request to service
      dataURL <- paste0( cdsURL, '/resources/', dataset)
      response <- POST(  dataURL, body = req, encode = 'json', authenticate( cdsUID, cdsPWD  )  )
      # Check for error
      if( http_error(response) ){
         print( content( response ) )
         stop()
      }
      request_id <- content(response)$request_id
   }else{
      source <- paste0( cdsURL, '/tasks/', request_id )
      response <- GET( source , authenticate(CDSuid, CDSpwd) )
   }
   # check if ready, loop until timeout if not
   if( content(response)$state != 'completed' ){
      waiting <- TRUE
      sleeptime = 1
      while( waiting ){
         # Check status
         source <- paste0( cdsURL, '/tasks/', request_id )
         response <- GET( source , authenticate(CDSuid, CDSpwd) )
         print( content( response ) )
         if( content( response )$state == 'completed' ){
            waiting <- FALSE
         }else if( content( response )$state %in% c('queued','running') ){
            Sys.sleep( sleeptime )
            sleeptime <- sleeptime * 1.5
         }else{
            waiting <- FALSE
         }
         if( sleeptime > 60)waiting <- FALSE
      }
   }
   # either time out or data ready
   if( content(response)$state == 'completed' ){
      # download the data
      if (target == ''){
         outfile = tempfile(pattern = "file", tmpdir = tempdir(), fileext = ".nc")
      }else{
         outfile = target
      }
      GET( url = content(response)$location , write_disk(outfile, overwrite = TRUE) )
   }else{
      print( "Timeout ..." )
      outfile = NA
   }
   return_value = list( request_id = request_id, data_file = outfile)
   return( return_value )
}
