const ModuloCRM = ({ user, theme, THEMES, themeKey, setThemeKey, clients, filters, setFilters, paginatedClients, totalPages, currentPage, setCurrentPage, setModal, setSelectedClient, fetchClients, pendingCount, showCalendar, calendarEvents, loadingCalendar, fetchMyCalendar, ESTADO_OPTIONS }) => {
    return (
        <div className="p-4 md:p-8 animate-in fade-in duration-500">
            <header className="max-w-6xl mx-auto flex flex-col md:flex-row justify-between items-center mb-10">
                <div className="flex items-center gap-5">
                    <div className="w-14 h-14 rounded-2xl flex items-center justify-center text-white shadow-xl" style={{ backgroundColor: theme.primary }}>
                        <i className="fas fa-server text-2xl"></i>
                    </div>
                    <div>
                        <h1 className="text-2xl font-black text-slate-800 tracking-tighter uppercase leading-none">CRM Asesoras</h1>
                        <p className="text-slate-400 text-[9px] font-bold uppercase tracking-[0.2em] mt-1.5">Asesora: <span className="text-slate-900">{user}</span> • Versión 6.1 Maestro</p>
                    </div>
                </div>

                <div className="flex items-center gap-4 mt-6 md:mt-0">
                    <div className={`flex items-center gap-3 px-5 py-2.5 rounded-full border transition-all ${pendingCount > 0 ? 'bg-amber-50 text-amber-600 border-amber-200 sync-active' : 'bg-emerald-50 text-emerald-600 border-emerald-200'}`}>
                        <i className={`fas ${pendingCount > 0 ? 'fa-cloud-upload-alt' : 'fa-check-circle'} text-xs`}></i>
                        <span className="text-[10px] font-black uppercase tracking-widest">{pendingCount > 0 ? `${pendingCount} Sincronizando` : 'DB Local al Día'}</span>
                    </div>

                    <div className="flex items-center gap-3 glass p-2 rounded-full shadow-sm border border-white">
                        {Object.keys(THEMES).map(t => (
                            <button key={t} onClick={() => { setThemeKey(t); localStorage.setItem('crm_theme', t); }} className={`w-6 h-6 rounded-full transition-all ${themeKey === t ? 'ring-2 ring-offset-2 ring-slate-300 scale-110 shadow-md' : 'opacity-30 hover:opacity-100'}`} style={{ backgroundColor: THEMES[t].primary }} />
                        ))}
                        <div className="w-px h-5 bg-slate-200 mx-1" />
                        <button onClick={() => { localStorage.clear(); window.location.reload(); }} className="text-slate-300 hover:text-red-500 p-1 transition-colors bg-transparent border-none outline-none"><i className="fas fa-power-off text-sm"></i></button>
                    </div>
                </div>
            </header>

            <main className="max-w-6xl mx-auto">
                <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-6 gap-4 mb-8">
                    <div className="lg:col-span-2 relative">
                        <i className="fas fa-search absolute left-5 top-1/2 -translate-y-1/2 text-slate-300 text-xs"></i>
                        <input type="text" placeholder="Buscar expediente..." className="w-full pl-12 pr-5 py-4 rounded-2xl border-none shadow-sm font-bold text-xs focus:ring-2 focus:ring-sky-300 outline-none transition-all" value={filters.name} onChange={e => setFilters({ ...filters, name: e.target.value })} />
                    </div>
                    <select className="px-5 py-4 rounded-2xl border-none shadow-sm font-bold text-xs text-slate-500 outline-none cursor-pointer" value={filters.dateRange} onChange={e => setFilters({ ...filters, dateRange: e.target.value })}>
                        <option value="">Semáforo</option>
                        <option value="vencido">Vencidos 🔴</option>
                        <option value="hoy">Para Hoy 🟠</option>
                        <option value="mañana">Mañana 🟡</option>
                        <option value="futuro">Próximos 🔵</option>
                    </select>
                    <select className="px-5 py-4 rounded-2xl border-none shadow-sm font-bold text-xs text-slate-500 outline-none cursor-pointer" value={filters.status} onChange={e => setFilters({ ...filters, status: e.target.value })}>
                        <option value="">Estado</option>
                        {ESTADO_OPTIONS.map(o => <option key={o} value={o}>{o}</option>)}
                    </select>
                    <button onClick={() => { setShowHistoryModal(true); }} className="bg-white text-slate-400 font-bold py-4 px-4 rounded-2xl shadow-sm hover:bg-slate-50 transition-all flex items-center justify-center gap-2 uppercase text-[10px] tracking-widest border border-slate-100 outline-none">
                        <i className="fas fa-history"></i> Sync Queue
                    </button>
                    <button onClick={() => setModal('new')} className="text-white font-black py-4 px-6 rounded-2xl shadow-lg hover:shadow-xl transition-all flex items-center justify-center gap-2 uppercase text-[10px] tracking-widest border-none outline-none" style={{ backgroundColor: theme.primary }}>
                        <i className="fas fa-plus-circle"></i> Nuevo
                    </button>
                </div>


                {/* MÓDULO DE AGENDA MAESTRA DESPLEGABLE */}
                <div className="mb-8 animate-in fade-in slide-in-from-top-4 duration-500">
                    <div
                        onClick={fetchMyCalendar}
                        className="glass p-6 rounded-[2.5rem] border border-white shadow-sm flex items-center justify-between cursor-pointer hover:bg-white/50 transition-all"
                    >
                        <div className="flex items-center gap-4">
                            <div className="w-10 h-10 rounded-2xl flex items-center justify-center text-white shadow-md" style={{ backgroundColor: theme.primary }}>
                                <i className={`fas ${showCalendar ? 'fa-calendar-minus' : 'fa-calendar-alt'} text-sm`}></i>
                            </div>
                            <div>
                                <h3 className="text-[10px] font-black uppercase text-slate-800 tracking-[0.15em] leading-none">Mi Agenda Maestra</h3>
                                <p className="text-[9px] font-bold text-slate-400 uppercase mt-1">Próximas citas y compromisos</p>
                            </div>
                        </div>
                        <div className="flex items-center gap-3">
                            {loadingCalendar && <i className="fas fa-circle-notch animate-spin text-slate-300"></i>}
                            <i className={`fas fa-chevron-${showCalendar ? 'up' : 'down'} text-slate-300 text-xs`}></i>
                        </div>
                    </div>

                    {/* PANEL EXPANDIBLE */}
                    {showCalendar && (
                        <div className="mt-4 grid grid-cols-1 md:grid-cols-3 gap-4 animate-in zoom-in-95 duration-300">
                            {calendarEvents.length === 0 && !loadingCalendar ? (
                                <div className="md:col-span-3 glass p-10 rounded-[2.5rem] text-center border border-white">
                                    <p className="text-[10px] font-black text-slate-300 uppercase tracking-widest">No hay citas programadas en el calendario maestro</p>
                                </div>
                            ) : (
                                calendarEvents.map((ev, i) => (
                                    <div key={i} className="bg-white p-5 rounded-[2rem] shadow-sm border border-slate-50 border-l-4" style={{ borderLeftColor: theme.primary }}>
                                        <div className="flex justify-between items-start mb-2">
                                            <p className="font-black text-slate-700 text-[11px] leading-tight uppercase">{ev.summary}</p>
                                            <i className="far fa-clock text-slate-200 text-[10px]"></i>
                                        </div>
                                        <p className="text-[10px] text-slate-400 font-bold italic mb-2">
                                            {new Date(ev.start).toLocaleString('es-MX', {
                                                day: '2-digit', month: 'short', hour: '2-digit', minute: '2-digit'
                                            })}
                                        </p>
                                        {ev.description && (
                                            <p className="text-[9px] text-slate-400 line-clamp-2 border-t border-slate-50 pt-2 mt-2">{ev.description}</p>
                                        )}
                                    </div>
                                ))
                            )}
                        </div>
                    )}
                </div>


                <div className="glass rounded-[3rem] shadow-2xl overflow-hidden border border-white">
                    <div className="overflow-x-auto custom-scroll">
                        <table className="w-full text-left table-auto">
                            <thead className="bg-slate-50/50 border-b border-slate-100 text-slate-400 text-[10px] uppercase font-black tracking-widest">
                                <tr>
                                    <th className="px-8 py-5">Prospecto</th>
                                    <th className="px-8 py-5">WhatsApp</th>
                                    <th className="px-8 py-5">Interés</th>
                                    <th className="px-8 py-5">Próx. Contacto</th>
                                    <th className="px-8 py-5">Estatus</th>
                                    <th className="px-8 py-5 text-right pr-12">Acción</th>
                                </tr>
                            </thead>
                            <tbody className="divide-y divide-slate-100">
                                {paginatedClients.length > 0 ? paginatedClients.map((c, i) => (
                                    <tr key={i} className="hover:bg-slate-50/50 transition-colors cursor-pointer" onClick={() => { setSelectedClient(c); setModal('edit'); }}>
                                        <td className="px-8 py-5">
                                            <p className="font-bold text-slate-700 text-xs">{c.Nombre}</p>
                                            <p className="text-[9px] text-slate-300 font-bold uppercase mt-1 tracking-tighter">ID: {c.id_unico}</p>
                                        </td>
                                        <td className="px-8 py-5 text-[11px] font-bold text-slate-500">{c.Canal || '--'}</td>
                                        <td className="px-8 py-5">
                                            <span className={`px-2.5 py-1 rounded-md text-[9px] font-black uppercase tracking-tighter ${(c['Nivel de Interés'] || '').includes('Alto') ? 'bg-orange-100 text-orange-600' : 'bg-slate-100 text-slate-400'
                                                }`}>{(c['Nivel de Interés'] || '').split(' <')[0] || 'N/A'}</span>
                                        </td>
                                        <td className={`px-8 py-5 text-[11px] font-black ${(() => {
                                            if (!c['Fecha Próx. Contacto'] || c['Fecha Próx. Contacto'] === '--') return 'text-slate-300';
                                            const [d, m, y] = c['Fecha Próx. Contacto'].split('/').map(Number);
                                            const pDate = new Date(y, m - 1, d);
                                            const nowMx = new Date(getMexicoDate() + "T00:00:00");
                                            if (pDate < nowMx) return 'text-red-500';
                                            if (pDate.getTime() === nowMx.getTime()) return 'text-orange-500';
                                            return 'text-sky-500';
                                        })()}`}>
                                            {c['Fecha Próx. Contacto'] || '--'}
                                        </td>
                                        <td className="px-8 py-5">
                                            <span className={`px-3 py-1.5 rounded-xl border text-[9px] font-black uppercase tracking-widest ${c['Estado Final'] === 'Venta' ? 'bg-green-50 text-green-700 border-green-200' :
                                                c['Estado Final'] === 'No interesado' ? 'bg-red-50 text-red-700 border-red-200' : 'bg-yellow-50 text-yellow-700 border-yellow-200'
                                                }`}>{c['Estado Final']}</span>
                                        </td>
                                        <td className="px-8 py-5 text-right pr-12">
                                            <div className="w-10 h-10 rounded-2xl bg-white shadow-sm flex items-center justify-center ml-auto hover:bg-sky-50 hover:text-sky-600 transition-all border border-slate-100">
                                                <i className="fas fa-eye text-slate-300 text-sm"></i>
                                            </div>
                                        </td>
                                    </tr>
                                )) : (
                                    <tr><td colSpan="6" className="py-24 text-center opacity-30 font-black uppercase text-xs tracking-[0.4em]">Sin Resultados en la Base Maestro</td></tr>
                                )}
                            </tbody>
                        </table>
                    </div>

                    {totalPages > 1 && (
                        <div className="p-8 flex justify-center items-center gap-3 bg-slate-50/30 border-t border-slate-100">
                            <button disabled={currentPage === 1} onClick={(e) => { e.stopPropagation(); setCurrentPage(p => p - 1) }} className="w-12 h-12 rounded-2xl flex items-center justify-center text-slate-400 hover:bg-white disabled:opacity-20 transition-all border-none outline-none"><i className="fas fa-chevron-left text-xs"></i></button>
                            {[...Array(totalPages)].map((_, i) => (
                                <button key={i} onClick={(e) => { e.stopPropagation(); setCurrentPage(i + 1) }} className={`w-12 h-12 rounded-2xl font-black text-xs transition-all border-none outline-none ${currentPage === i + 1 ? 'text-white shadow-xl' : 'text-slate-400 hover:bg-white'}`} style={currentPage === i + 1 ? { backgroundColor: theme.primary } : {}}>{i + 1}</button>
                            ))}
                            <button disabled={currentPage === totalPages} onClick={(e) => { e.stopPropagation(); setCurrentPage(p => p + 1) }} className="w-12 h-12 rounded-2xl flex items-center justify-center text-slate-400 hover:bg-white disabled:opacity-20 transition-all border-none outline-none"><i className="fas fa-chevron-right text-xs"></i></button>
                        </div>
                    )}
                </div>
            </main>
        </div>
    );
};