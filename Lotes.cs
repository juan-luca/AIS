using BusComun;
using Comun;
using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;

namespace BusInchost
{

    public class Lotes : cBase
    {

        public Lotes(cConexion pconn)
        {
            conn = pconn;
        }

        public bool ProcesoLotes()
        {
            return ProcesoMasivo();
            //return ProcesoUnitario();
        }

        #region Proceso Masivo

        /// <summary>
        /// Alta y actualización masiva de Lotes y obligacion lider
        /// </summary>
        private bool ProcesoMasivo()
        {
            bool ret = true;

            LotesDalc LotesD = new LotesDalc(conn);

            // 1) Crea los lotes de los clientes T01 que no tengan lote
            cIncidencia.Aviso("Inicio de creación de nuevos lotes");
            ret = LotesD.InsertarLotesNuevos();
            if (LotesD.Errores.Cantidad() > 0 || !ret)
            {
                this.Errores.Agregar(LotesD.Errores);
                cIncidencia.Generar(LotesD.Errores, "InsertarLotesNuevos");
                return ret;
            }

            // 2) Inicializa todas las obligacones lider y marca la nueva obligación lider de cada lote
            cIncidencia.Aviso("Calculo de obligación lider");
            ret = LotesD.ObligacionesLider();
            if (LotesD.Errores.Cantidad() > 0 || !ret)
            {
                this.Errores.Agregar(LotesD.Errores);
                cIncidencia.Generar(LotesD.Errores, "ObligacionesLider");
                return ret;
            }

            // 3) Actualiza las variables resumen del lote
            cIncidencia.Aviso("Inicio de Actualizaicón de lotes");
            ret = LotesD.ActualizacionLotes();
            if (LotesD.Errores.Cantidad() > 0 || !ret)
            {
                this.Errores.Agregar(LotesD.Errores);
                cIncidencia.Generar(LotesD.Errores, "ActualizacionLotes");
                return ret;
            }

            // 4) Cálculo de prioridad de ordenación del lotes (agenda motor)
            cIncidencia.Aviso("Calculo de prioridad de los lotes");
            ret = LotesD.AsignarPrioridadLote();
            if (LotesD.Errores.Cantidad() > 0 || !ret)
            {
                this.Errores.Agregar(LotesD.Errores);
                cIncidencia.Generar(LotesD.Errores, "AsignarPrioridadLote");
                return ret;
            }

            return ret;
        }

        #endregion

        #region Proceso Unitario

        private bool ProcesoUnitario()
        {
            try
            {
                long nContTrat = 0;
                long nContOK = 0;
                string sClienteAnt = "-1";

                LotesDalc LotesD = new LotesDalc(conn);

                TyObligLote Obligacion = new TyObligLote();

                cGlobales cGbl = new cGlobales(conn);
                cGbl.CargoTablaRSEGM();
                if (cGbl.Errores.Cantidad() != 0)
                {
                    cIncidencia.Generar(cGbl.Errores, "GETRESGM", "Error al recuperar registros de la tabla RSEGM");
                    return false;
                }

                List<TyObligLote> ObligacionesDelLote = new List<TyObligLote>();

                AISDataReader DrC = new AISDataReader();

                // Uso una segunda conexion a la base para el DataReader Principal
                cConexion connLotes = new cConexion(cGlobales.XMLConex);
                connLotes.Conectar();
                if (connLotes.Errores.Cantidad() != 0)
                {
                    cIncidencia.Generar(connLotes.Errores, "OPENBDLOTE", "No se pudo abrir la conexion");
                    return false;
                }

                cIncidencia.Aviso("Comienza apertura del Cursor de lotes");

                //Abro el cursor de Lotes recibidas en la tabla de intercambio
                LotesDalc vLoteBD = new LotesDalc(connLotes);
                DrC = vLoteBD.AbrirCursorObligaciones();

                if (vLoteBD.Errores.Cantidad() != 0)
                {
                    cIncidencia.Generar(vLoteBD.Errores, "OPENLOTE", "Error al abrir cursor de Lotes");
                    return false;
                }

                cIncidencia.Aviso("Finaliza apertura del Cursor de lotes");

                // Establezco la cantidad de hilos que se van a utilizar y los creo 
                Hilos HilosLote = new Hilos(cGlobales.nLotesXHilo, cGlobales.nHilosLotes);

                // Conecto los hilos a la base de datos
                if (!HilosLote.ConectarHilosABaseDeDatos(cGlobales.XMLConex))
                    return false;

                while (vLoteBD.FechObligaciones(DrC, ref Obligacion))
                {
                    nContTrat++;

                    if (sClienteAnt != Obligacion.OCRAIZ)
                    {
                        sClienteAnt = Obligacion.OCRAIZ;
                        if (ObligacionesDelLote.Count > 0)
                        {
                            // Cargo el elemento en alguno de los hilos disponible
                            // Si la CargoElementoEnAlgunHilo retorna falso entonces indica que los hilos completos 
                            // y listos para ejecutar
                            if (!HilosLote.CargoElementoEnAlgunHilo(ObligacionesDelLote))
                            {
                                HilosLote.Arrancar(new ParameterizedThreadStart(FuncionArranqueHilo));
                            }
                        }

                        ObligacionesDelLote = new List<TyObligLote>();
                    }

                    ObligacionesDelLote.Add(Obligacion);
                    cIncidencia.SetMarcaVelocidad(nContTrat);
                }

                /* Cargo los Direcciones del ultimo cliente*/
                if (ObligacionesDelLote.Count > 0)
                    HilosLote.CargoElementoEnAlgunHilo(ObligacionesDelLote);

                HilosLote.Arrancar(new ParameterizedThreadStart(FuncionArranqueHilo));

                DrC.Close();
                connLotes.Desconectar();

                if (vLoteBD.Errores.Cantidad() != 0)
                    return false;
                else
                {
                    if (HilosLote.EjecucionDeLosHilosOk())
                    {
                        nContTrat = HilosLote.ElemenosTratados();
                        nContOK = HilosLote.ElemenosTratadosOk();

                        HilosLote.CommitDeLosHilos();
                        HilosLote.DesConectarHilosABaseDeDatos();
                        cIncidencia.Aviso("Lotes: Tratados -> " + nContTrat.ToString() + " OK -> " + nContOK);
                        return true;
                    }
                    else
                    {
                        HilosLote.RollbackDeLosHilos();
                        HilosLote.DesConectarHilosABaseDeDatos();
                        return false;
                    }
                }
            }
            catch (Exception e)
            {
                cIncidencia.Aviso("Se produjo un error inesperado en la administracion de hilos. " + e.Message);
                return false;
            }
        }

        public void FuncionArranqueHilo(object obj)
        {
            Lotes LoteActual;
            HiloAIS Hilo;


            Hilo = (HiloAIS)obj;
            LoteActual = new Lotes(Hilo.Conexion);

            LoteActual.TratarListaLotes(obj);

            return;
        }

        public void TratarListaLotes(object obj)
        {
            HiloAIS Hilo;

            Hilo = (HiloAIS)obj;

            List<object> ListaLotes = Hilo.ListaElementosAIS;

            foreach (List<TyObligLote> ObligacionesDelLote in ListaLotes)
            {
                Hilo.nContTratados++;
                if (TrataLote(ObligacionesDelLote))
                    Hilo.nContOk++;

                if ((Hilo.nContTratados % cGlobales.nFrecuenciaCommit) == 0)
                {
                    conn.CommitTransaccion();
                    conn.ComienzoTransaccion();
                }

            }
            Hilo.bEjecutadoOk = (Hilo.bEjecutadoOk && true);

            return;
        }

        private bool TrataLote(List<TyObligLote> ObligacionesDelLote)
        {
            int nNumElemsLote = 0;
            double nMoraTotal = 0;
            double nSalCapAcum = 0;
            double nLODEUDATOTAL = 0;
            double nDEUDATOTALUSD = 0;
            
            bool bNuevoLote = true;
            bool bTieneLider = false;
            bool bLoteOK = true;
            string sCodLote = "";
            string sCodLoteActual = "";
            bool bExisteLote = false;
            TyLote vLote = new TyLote();

            LotesDalc LotesD = new LotesDalc(conn);

            foreach (TyObligLote Obligacion in ObligacionesDelLote)
            {
                sCodLote = Obligacion.OCRAIZ;
                sCodLoteActual = Obligacion.LOTEACTUAL;

                /* Aumentamos el número de elementos del lote si la obligación está en mora */
                if (Obligacion.DEDIAS > 0)
                {
                    nNumElemsLote++;
                    nMoraTotal += Obligacion.MORATOTAL;
                    nSalCapAcum += Obligacion.SALCAP;
                    nLODEUDATOTAL += Obligacion.DEUDATOTAL;
                    nDEUDATOTALUSD += Obligacion.DEMORATOTUSD;
                }

                if (Obligacion.OGLOTE != "" && Obligacion.OGLOTE == Obligacion.OCRAIZ)
                    bNuevoLote = false;

                /* Marcar el líder: es la primera con fecha de mora no vacía */
                /* las que no son líder, las marca como no líder */
                if (!bTieneLider)
                {
                    bTieneLider = true;
                    // Verifico que la obligacion ya no sea lider y de ese lote
                    if (!(Obligacion.OGLIDER == "1" && Obligacion.OGLOTE != "" && Obligacion.OGLOTE == Obligacion.OCRAIZ))
                    {
                        if (!LotesD.UpdateLider(Obligacion.OGCOD, Obligacion.OCRAIZ, true))
                            bLoteOK = false;
                    }
                }
                else
                {
                    if (!(Obligacion.OGLIDER == "0" && Obligacion.OGLOTE != "" && Obligacion.OGLOTE == Obligacion.OCRAIZ))
                    {
                        if (!LotesD.UpdateLider(Obligacion.OGCOD, Obligacion.OCRAIZ, false))
                            bLoteOK = false;
                    }
                }
            }

            //Comprobamos si el Código de Lote actual corresponde a un Lote existente
            bExisteLote = (sCodLoteActual == sCodLote);

            if (!bLoteOK)
                return false;

            /* Si se ha creado un nuevo lote, se procede a montarlo en la BD */
            if ((bNuevoLote) && (!bExisteLote))
            {
                vLote.LOCOD = sCodLote;
                vLote.LOPERFIL = "";
                vLote.LONMORA = "0";
                vLote.LOROL = "";
                vLote.LOTURNO = "";
                vLote.LOESTRATEG = "";
                vLote.LOPRIORIDAD = 0.0;
                vLote.LODEUDA = nMoraTotal;
                vLote.LONUMELEMS = nNumElemsLote;
                vLote.LOESTADO = Const.OBLG_MARCA_REGISTRO_ALTA;
                vLote.LOFECALTA = cGlobales.Hoy;
                vLote.LOFECPERFIL = cGlobales.Hoy;
                vLote.LOFECROL = cGlobales.Hoy;
                vLote.LOFECNMORA = cGlobales.Hoy;
                vLote.LOFECTURNO = cGlobales.Hoy;
                vLote.LODEUDATOTAL += nLODEUDATOTAL;
                vLote.LODEUDAUSD += nDEUDATOTALUSD;
                vLote.LOPRIORIDAD = CalcularPrioridad(ObligacionesDelLote[0].DEDIAS, nMoraTotal, nSalCapAcum);

                bLoteOK = LotesD.InsertarLote(vLote);
            }
            else
            {
                vLote.LOCOD = sCodLote;
                vLote.LOPRIORIDAD = CalcularPrioridad(ObligacionesDelLote[0].DEDIAS, nMoraTotal, nSalCapAcum);
                vLote.LODEUDA = nMoraTotal;
                vLote.LONUMELEMS = nNumElemsLote;
                vLote.LOESTADO = Const.OBLG_MARCA_MODIF;
                vLote.LODEUDATOTAL += nLODEUDATOTAL;
                vLote.LODEUDAUSD += nDEUDATOTALUSD;

                bLoteOK = LotesD.UpdateCamposLote(vLote);
            }

            return bLoteOK;
        }

        private double CalcularPrioridad(long nDias, double Mora, double Saldo)
        {
            const string VAR_MONTO_DEUDA = "1";
            const string VAR_ANTIGUEDAD = "2";
            const string VAR_MONTO_SALDO = "3";
            double nPrioridad = 0;
            double nResultDeuda = 0;
            double nResultSaldo = 0;
            double nResultAntig = 0;


            nResultDeuda = GetValorTramo(VAR_MONTO_DEUDA, Mora);
            nResultSaldo = GetValorTramo(VAR_MONTO_SALDO, Saldo);
            nResultAntig = GetValorTramo(VAR_ANTIGUEDAD, Convert.ToDouble(nDias));

            nPrioridad = nResultDeuda * nResultAntig * nResultSaldo;

            return nPrioridad;
        }

        private double GetValorTramo(string sVariable, double Valor)
        {
            try
            {
                string sSql;
                string sOrder;
                double nRetorno = 0;

                sSql = "SGVARIABLE = " + cFormat.StToBD(sVariable);
                sSql += " AND SGFINTRM > " + Valor.ToString();

                sOrder = "SGFINTRM ASC";

                DataRow[] Filas = cGlobales.DsGbl.Tables["RSEGM"].Select(sSql, sOrder);

                nRetorno = Convert.ToDouble(Filas[0]["SGVALTRM"].ToString());

                return nRetorno;

            }
            catch
            {
                return 1;
            }

        }

        #endregion

        #region Asignación de Estrategias

        public bool AsignarEstrategias()
        {
            try
            {
                string sEstrategia = "";
                string sGrupo = "";
                int nCantLote = 0;
                int i = 0;

                LotesDalc LotesD = new LotesDalc(conn);

                cIncidencia.Aviso("Inicializando loestrateg lotes - " + DateTime.Now.ToString("HH:mm:ss:fff"));
                LotesD.InicializaEstrategiaLotes();
                cIncidencia.Aviso("FIN Inicializando loestrateg lotes - " + DateTime.Now.ToString("HH:mm:ss:fff"));

                DataSet Ds = LotesD.GetAsigEstrategiasPendientes();
                if (LotesD.Errores.Cantidad() != 0)
                    return false;

                foreach (DataRow Row in Ds.Tables["ASESTPDTE"].Rows)
                {
                    i++;
                    sEstrategia = Row["ETESTRATEG"].ToString().Trim().ToUpper();
                    sGrupo = Row["ETGRUPO"].ToString().Trim().ToUpper();
                    nCantLote = Convert.ToInt32(cFormat.NumBDToPC(Row["DISTRIBUIR"].ToString()));

                    if (i == Ds.Tables["ASESTPDTE"].Rows.Count)
                    {
                        nCantLote = 99999999;
                    }
                    else if (Ds.Tables["ASESTPDTE"].Rows[i]["ETGRUPO"].ToString().Trim().ToUpper() != sGrupo)
                    {
                        nCantLote = 99999999;
                    }

                    if (nCantLote > 0)
                    {
                        if (!LotesD.ActualizarEstrategia(sGrupo, sEstrategia, nCantLote))
                            return false;
                    }
                }

                return true;

            }
            catch (Exception e)
            {
                cIncidencia.Aviso("Se produjo un error inesperado al resolver asignacion de estrategias. " + e.Message);
                return false;
            }
        }

        #endregion

    }
}
