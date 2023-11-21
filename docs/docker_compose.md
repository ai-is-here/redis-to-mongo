compose.sh script will select the appropriate Docker Compose and environment files based on the argument (test or prod) and manage the Docker environment accordingly.

### Usage

1. Make the script executable:

   ```bash
   chmod +x compose.sh
   ```

2. To start the test environment:

   ```bash
   ./compose.sh test up
   ```

3. To stop the test environment:

   ```bash
   ./compose.sh test down
   ```

4. To start the production environment:

   ```bash
   ./compose.sh prod up
   ```

5. To stop the production environment:
   ```bash
   ./compose.sh prod down
   ```

### Notes

- Ensure you have the corresponding Docker Compose files (`docker-compose.test.yml`, `docker-compose.prod.yml`) and environment files (`.env.test`, `.env.prod`) set up in your project.
- The script checks if the Docker Compose environment is already running before starting it again to avoid relaunching an already running environment.
- This script assumes a certain naming convention for the Compose and environment files. Adjust the file names in the script if your project uses different names.
