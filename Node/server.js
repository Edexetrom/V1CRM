const express = require('express');
const { createProxyMiddleware } = require('http-proxy-middleware');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 80;

/**
 * MIDDLEWARE DE PARSEO (CRÍTICO PARA 25 USUARIOS)
 * Definimos un límite de 25MB para soportar múltiples fotos en Base64 simultáneamente.
 */
app.use(express.json({ limit: '25mb' }));
app.use(express.urlencoded({ limit: '25mb', extended: true }));

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
        /**
         * PARCHE DE CUERPO DE PETICIÓN (Body-Parser Fix)
         * Cuando usamos express.json(), el stream del body se "consume".
         * Esta lógica re-inyecta el body en la petición del proxy para que llegue al backend de Python.
         */
        if (req.body && Object.keys(req.body).length > 0) {
            const bodyData = JSON.stringify(req.body);
            proxyReq.setHeader('Content-Type', 'application/json');
            proxyReq.setHeader('Content-Length', Buffer.byteLength(bodyData));
            proxyReq.write(bodyData);
        }
    }
}));

/**
 * LÓGICA DE ENRUTAMIENTO POR DOMINIO
 */
app.get('/', (req, res) => {
    const host = req.headers.host;
    if (host && host.includes('crmauditores.libresdeumas.com')) {
        res.sendFile(path.join(__dirname, 'audit.html'));
    } else {
        res.sendFile(path.join(__dirname, 'index.html'));
    }
});

/**
 * SERVICIO DE ARCHIVOS ESTÁTICOS
 */
app.use(express.static(path.join(__dirname, '.')));

/**
 * SOPORTE PARA REFRESH (CATCH-ALL)
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
    console.log(`Servidor Multi-CRM (Optimizado v5.1) activo en puerto ${PORT}`);
    console.log(`Límite de carga: 25MB`);
});