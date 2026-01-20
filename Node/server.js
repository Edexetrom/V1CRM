const express = require('express');
const { createProxyMiddleware } = require('http-proxy-middleware');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 80;

/**
 * CONFIGURACIÓN DEL PROXY PARA LA API
 * Redirige las peticiones /api al contenedor de Python (backend)
 */
app.use('/api', createProxyMiddleware({
    target: 'http://crmasesorasapi.libresdeumas.com',
    changeOrigin: true,
    pathRewrite: {
        '^/api': '/api',
    },
    onProxyReq: (proxyReq, req, res) => {
        // Manejo de cuerpos de petición grandes para imágenes
        if (req.body) {
            let bodyData = JSON.stringify(req.body);
            proxyReq.setHeader('Content-Type', 'application/json');
            proxyReq.setHeader('Content-Length', Buffer.byteLength(bodyData));
            proxyReq.write(bodyData);
        }
    }
}));

/**
 * LÓGICA DE ENRUTAMIENTO POR DOMINIO
 * Detecta el host para servir el CRM de Asesoras o el de Auditores
 */
app.get('/', (req, res) => {
    const host = req.headers.host;

    if (host && host.includes('crmauditores.libresdeumas.com')) {
        // Si el dominio es de auditores, sirve audit.html
        res.sendFile(path.join(__dirname, 'audit.html'));
    } else {
        // Por defecto (o si es crmasesoras), sirve index.html
        res.sendFile(path.join(__dirname, 'index.html'));
    }
});

/**
 * SERVICIO DE ARCHIVOS ESTÁTICOS
 */
app.use(express.static(path.join(__dirname, '.')));

/**
 * SOPORTE PARA REFRESH (CATCH-ALL)
 * Repite la lógica del host para que las rutas internas funcionen
 */
app.get('*', (req, res) => {
    const host = req.headers.host;
    if (host && host.includes('crmauditores.libresdeumas.com')) {
        res.sendFile(path.join(__dirname, 'audit.html'));
    } else {
        res.sendFile(path.join(__dirname, 'index.html'));
    }
});

app.listen(PORT, () => {
    console.log(`Servidor Multi-CRM activo en puerto ${PORT}`);
    console.log(`Asesoras: crmasesoras.libresdeumas.com`);
    console.log(`Auditores: crmauditores.libresdeumas.com`);
});