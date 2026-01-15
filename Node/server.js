const express = require('express');
const { createProxyMiddleware } = require('http-proxy-middleware');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 80;

// Configuración del Proxy para la API
// Redirige todo lo que llegue a /api hacia el contenedor 'backend'
app.use('/api', createProxyMiddleware({
    target: 'http://crmasesorasapi.libresdeumas.com',
    changeOrigin: true,
    pathRewrite: {
        '^/api': '/api', // Mantiene el prefijo /api
    },
    onProxyReq: (proxyReq, req, res) => {
        // Ajuste para manejar cuerpos de petición grandes (imágenes)
        if (req.body) {
            let bodyData = JSON.stringify(req.body);
            proxyReq.setHeader('Content-Type', 'application/json');
            proxyReq.setHeader('Content-Length', Buffer.byteLength(bodyData));
            proxyReq.write(bodyData);
        }
    }
}));

// Servir archivos estáticos
// Buscamos el index.html en la raíz del directorio
app.use(express.static(path.join(__dirname, '.')));

// Cualquier otra ruta sirve el index.html (soporte para Single Page Apps)
app.get('*', (req, res) => {
    res.sendFile(path.join(__dirname, 'index.html'));
});

app.listen(PORT, () => {
    console.log(`Servidor Node.js activo en puerto ${PORT}`);
    console.log(`Proxy configurado: /api -> http://crmasesorasapi.libresdeumas.com`);
});