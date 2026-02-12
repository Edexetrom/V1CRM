const ModuloCalendarios = () => {
    const [data, setData] = React.useState([]);

    // Aquí él puede hacer sus propios fetch a la API
    React.useEffect(() => {
        // fetch('/api/sus-datos')...
    }, []);

    return (
        <div className="p-8 bg-white rounded-[2rem] shadow-sm">
            <h2 className="font-bold text-slate-800">Sistema del Colaborador</h2>
            {/* Todo su desarrollo aquí */}
        </div>
    );
};