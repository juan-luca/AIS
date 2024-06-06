using BusComun;
using Comun;
using System;
using System.Data;

namespace BusInchost
{
    public class Pagos : cBase
    {
        public Pagos(cConexion pconn)
        {
            conn = pconn;
        }

        /// <summary>
        /// Inserta pagos mediante Insert-select
        /// </summary>
        public bool ProcesoPagos()
        {
            bool res = false;

            PagosDalc pagos = new PagosDalc(conn);

            res = pagos.InsertPagos();

            return res;
        }

        public bool CruzarPagos()
        {
            long nContPagosTrat = 0;
            long nContPagosOK = 0;

            TyPago Pago = new TyPago();

            AISDataReader DrP = new AISDataReader();

            // Uso una segunda conexion a la base para el DataReader Principal
            cConexion connPagos = new cConexion(cGlobales.XMLConex);
            connPagos.Conectar();

            if (connPagos.Errores.Cantidad() != 0)
            {
                cIncidencia.Generar(connPagos.Errores, "OPENBD", "No se pudo abrir la conexion");
            }

            //Abro el cursor de Pagos recibidos en la tabla de intercambio
            PagosDalc vPagosBD = new PagosDalc(connPagos);
            DrP = vPagosBD.AbrirCursorNuevosPagos();

            if (vPagosBD.Errores.Cantidad() != 0)
            {
                cIncidencia.Generar(vPagosBD.Errores, "OPENPAG", "Error al abrir cursor de Convenios/Compromisos");
                return false;
            }

            while (vPagosBD.FechPago(DrP, Pago))
            {
                // Para cada pago lo trato
                nContPagosTrat++;
                if (Pago.nConv > 0 || Pago.Compromiso > 0)
                    if (CruzarPago(Pago))
                        nContPagosOK++;

                cIncidencia.SetMarcaVelocidad(nContPagosTrat);
            }

            DrP.Close();

            //Actualizamos el estado de los convenios
            PagosDalc pagod = new PagosDalc(conn);
            pagod.ActualizaAcuerdos();

            connPagos.Desconectar();

            if (vPagosBD.Errores.Cantidad() != 0)
                return false;
            else
            {
                cIncidencia.Aviso("Pago cruzados contra Compromisos/Convenios: Tratados -> " + nContPagosTrat.ToString() + " OK -> " + nContPagosOK.ToString());
                return true;
            }
        }

        private bool CruzarPago(TyPago Pago)
        {
            double nCantidadPagada = 0;
            double nCantxPagar = 0;
            double nPago = 0;
            double nPagado = 0;

            TyRCPago vRCPago = new TyRCPago();

            PagosDalc PagosD = new PagosDalc(conn);

            if (Pago.nConv > 0)
                TrataPagoConvenioSiCorresponde(Pago);

            // Si hay compromiso cruzamos los compromisos
            if (Pago.Compromiso > 0)
            {
                // Comienzo el cruce de los pagos contra las promesas de pago.
                vRCPago.CPOBLIG = Pago.PGOBLIG;
                vRCPago.CPFECVEN = Pago.PGFECVAL;

                DataSet Ds = PagosD.AbrirCursorRCPagosBD(vRCPago);

                if (PagosD.Errores.Cantidad() != 0)
                {
                    Errores.Agregar(PagosD.Errores.Item(0).nCodigo, PagosD.Errores.Item(0).sDescripcion, "TrataPago", Const.SEVERIDAD_Alta);
                    cIncidencia.Generar(Errores, "OPCURRCPAGO", "Error al obtener promesas de pago para la obligacion " + Pago.PGOBLIG);
                    return false;
                }

                nCantidadPagada = Pago.PGTOTAL;

                foreach (DataRow dr in Ds.Tables["RCPAGO"].Rows)
                {
                    if (nCantidadPagada <= 0)
                        break;
                    else
                    {
                        vRCPago.CPIDPROM = dr["CPIDPROM"].ToString();
                        vRCPago.CPFECVEN = dr["CPFECVEN"].ToString();
                        vRCPago.CPCANCOM = Convert.ToDouble(dr["CPCANCOM"].ToString());
                        vRCPago.CPCANPAG = Convert.ToDouble(dr["CPCANPAG"].ToString());
                        vRCPago.CPPERFIL = dr["CPPERFIL"].ToString();

                        nCantxPagar = vRCPago.CPCANCOM - vRCPago.CPCANPAG;
                        nPago = Math.Min(nCantxPagar, nCantidadPagada);
                        nPagado = vRCPago.CPCANPAG + nPago;

                        vRCPago.CPCANPAG = nPagado;
                        if (!PagosD.ActualizaRCPagoBD(vRCPago, Pago.PGFECVAL.ToString()))
                            break;

                        /* Actualizamos el valor de la cantidad pagada en total */
                        nCantidadPagada = nCantidadPagada - nPago;
                        /* Obtenemos los datos del siguiente compromiso */
                    }
                }
            }

            return true;
        }


        /// <summary>
        /// Se determinan el usuario de recupero y al nivel de mora al los cuales se le asociará el pago
        /// </summary>
        /// <param name="Pago"></param>
        private void AnalizarUsuarioRecupero(TyPago Pago)
        {
            PagosDalc PagosD = new PagosDalc(conn);

            PagosD.RecuperarUsuarioPagoBD(Pago);

            if (Pago.PGPERFIL == "")
                PagosD.RecuperarUsuarioPagoEstandarBD(Pago);
        }

        private bool TrataPagoConvenioSiCorresponde(TyPago Pago)
        {
            DataSet Dr = new DataSet();
            double nCantPagada = 0;

            TySaldoCP vSaldoCP = new TySaldoCP();

            PagosDalc vPagosD = new PagosDalc(conn);
           
            nCantPagada = Pago.PGTOTAL;

            Dr = vPagosD.AbrirCursorPendPagoCuotasConv(Pago.nConv);

            if (vPagosD.Errores.Cantidad() != 0)
            {
                cIncidencia.Generar(vPagosD.Errores, "OPENPENDPAGOCUOTASCONV", "Error abriendo el cursor convenio " + Pago.nConv.ToString());
                return false;
            }

            int i = 0;

            while ((i < Dr.Tables["RCUOTASCONVP"].Rows.Count) && (nCantPagada > 0))
            {
                vPagosD.FetchPendPagoCuotasConv(Dr.Tables["RCUOTASCONVP"].Rows[i], vSaldoCP);
                ActualizarSaldoCuotaConv(ref nCantPagada, Pago, vSaldoCP);
                i++;
            }

            return true;
        }

        private bool ActualizarSaldoCuotaConv(ref double nCantPagada, TyPago Pago, TySaldoCP vSaldoCP)
        {
            double nImputadoPago = 0;

            if (nCantPagada >= vSaldoCP.CUSALDOPENDCUOTA)
            {
                nCantPagada = nCantPagada - vSaldoCP.CUSALDOPENDCUOTA;
                nImputadoPago = vSaldoCP.CUSALDOPENDCUOTA;
                vSaldoCP.CUSALDOPEND = 0;
            }
            else
            {
                nImputadoPago = nCantPagada;
                vSaldoCP.CUSALDOPENDCUOTA = vSaldoCP.CUSALDOPENDCUOTA - nCantPagada;
                nCantPagada = 0;
            }

            PagosDalc PagosDalc = new PagosDalc(conn);

            if (!PagosDalc.ActualizoSaldoCuotaConvBD(vSaldoCP))
                return false;

            TyPagoConvP PagoConvP = new TyPagoConvP
            {
                PGCONV = vSaldoCP.CUCONV,
                PGIDPAGO = Pago.PGIDPAGO,
                PGCUOTACONV = Convert.ToInt32(vSaldoCP.CUCUOTA),
                PGTOTAL = nImputadoPago,
                PGFECVAL = Pago.PGFECVAL
            };

            if (!PagosDalc.InsertoPagoConvenioBD(PagoConvP))
                return false;

            /*
                        tyPago vPagoAux = new tyPago();

                        vPagoAux.PGOBLIG = vSaldoCP.CUOBLG;
                        vPagoAux.PGFECVAL = Pago.PGFECVAL;
                        vPagoAux.PGFECENT = Pago.PGFECENT;
                        vPagoAux.PGFECSIS = Pago.PGFECSIS;
                        vPagoAux.PGHORAPAGO = Pago.PGHORAPAGO;
                        vPagoAux.PGNROCUOTA = vSaldoCP.CUCUOTA.ToString();
                        vPagoAux.PGIDPAGO = GetIDPago(vPagoAux);
                        vPagoAux.PGTOTAL = nImputadoPago;
                        vPagoAux.PGCAPITAL = 0;
                        vPagoAux.PGINTCOR = 0;
                        vPagoAux.PGINTPUN = 0;
                        vPagoAux.PGOTROS = 0;

                        TrataPago(vPagoAux);
            */
            return true;

        }

        
    }
}
