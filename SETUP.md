# Project Setup

After installing the required programs in `project_requirements.txt`,<br> 
run the command <br> 
`pip install -r requirements.txt`<br>
to install the required Python libraries.

Then run the Python script `pipeline.py` to download the datasets, perform cleaning/pruning, and database creation/loading.

This may take a while depending on your internet connection speed, processing power, and mysql configuration settings.<br>
You may need to change the `max_allowed_packet` variable to a larger value in your mysql configuration file if you encounter a connection lost error during the pipeline script (restart the server service for the change to take effect).