const path = require('node:path')

const projectRoot = __dirname
const studioRoot = path.join(projectRoot, 'studio')

module.exports = {
  apps: [
    {
      name: 'aiquantbase-backend',
      cwd: projectRoot,
      script: '/home/mubin/.miniconda3/envs/amazing_data/bin/python',
      args: '-m aiquantbase.cli studio --host 0.0.0.0 --port 8000',
      interpreter: 'none',
      env: {
        PYTHONPATH: path.join(projectRoot, 'src'),
      },
    },
    {
      name: 'aiquantbase-studio',
      cwd: studioRoot,
      script: 'npm',
      args: 'run preview -- --host 0.0.0.0 --port 3000',
      env: {
        NUXT_BACKEND_BASE: 'http://127.0.0.1:8000',
      },
    },
  ],
}
