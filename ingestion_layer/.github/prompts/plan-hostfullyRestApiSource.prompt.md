## Step-by-Step Plan (Refined for Hostfully Production)

1. **Set the API Base URL**
   - Use the production base URL: `https://api.hostfully.com/api/v3.2/`.

2. **Configure Authentication**
   - Set the header `X-HOSTFULLY-APIKEY` with the value from `api_key` in `.dlt/secrets.toml`.

3. **Define the Leads Resource**
   - Add a resource for the `leads` endpoint (e.g., `/leads`).
   - Use cursor-based pagination:
     - Query parameters: `_limit` and `_cursor`.
     - Extract the next page cursor from the response at `_paging._nextCursor`.
   - Set the `data_selector` to extract the leads array from the response.

4. **Skip Incremental Loading**
   - Do not add incremental loading configuration for now.

5. **Pipeline Configuration**
   - Ensure the pipeline is named `hostfully_pipeline`.
   - Confirm the pipeline can be run with `python hostfully_pipeline.py`.

6. **Testing and Output**
   - Print the load information after running the pipeline for user inspection.

**Let me know if you want to further refine or approve this plan before implementation.**
